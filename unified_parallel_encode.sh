#!/bin/bash

# FFmpeg並列エンコード - 統合スクリプト
# 使用法: ./unified_parallel_encode.sh <入力ファイル名> [並列数] [CRF] [preset] [keyint]

# 固定設定
S3_BUCKET="xxx"
SEGMENT_LENGTH=60  # 1分 = 60秒

# ========================================
# 関数定義
# ========================================

# 使用法を表示
show_usage() {
    cat << EOF
使用法: $0 <入力ファイル名> [並列数] [CRF] [preset] [keyint]

必須引数:
  入力ファイル名: input/内のファイル名 (例: video.mkv)

オプション引数:
  並列数: 同時実行ジョブ数 (デフォルト: 4)
  CRF: Constant Rate Factor (デフォルト: なし)
  preset: エンコード速度プリセット (デフォルト: なし)
  keyint: キーフレーム間隔 (デフォルト: なし)

例:
  $0 video.mkv
  $0 video.mkv 8 23 medium 240
EOF
}

# タイムスタンプ付きログ出力
log_message() {
    local message="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $message"
}

# エラー処理
handle_error() {
    local message="$1"
    local exit_code="${2:-1}"
    log_message "エラー: $message"
    [ -n "$WORK_DIR" ] && [ -d "$WORK_DIR" ] && log_message "作業ディレクトリ: $WORK_DIR"
    exit "$exit_code"
}

# 入力パラメータの検証
validate_parameters() {
    # 必須パラメータチェック
    if [ -z "$INPUT_FILENAME" ]; then
        show_usage
        exit 1
    fi
}

# 作業ディレクトリとログの初期化
initialize_workspace() {
    # 入力ファイル名から作業ディレクトリ名を生成
    local input_basename=$(basename "$INPUT_FILENAME" | sed 's/\.[^.]*$//')
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    WORK_DIR="encode_${input_basename}_${timestamp}"
    mkdir -p "$WORK_DIR"
    
    # サブディレクトリ作成
    SEGMENTS_DIR="$WORK_DIR/segments"
    LOGS_DIR="$WORK_DIR/logs"
    mkdir -p "$SEGMENTS_DIR" "$LOGS_DIR"
    
    # ログファイルの設定
    LOG_FILE="$WORK_DIR/encode.log"
    exec 1> >(tee -a "$LOG_FILE")
    exec 2>&1
    
    # ローカルファイルパスを設定
    LOCAL_INPUT_FILE="$WORK_DIR/input_video"
    LOCAL_OUTPUT_FILE="$WORK_DIR/$INPUT_FILENAME"
}

# ヘッダー情報の出力
print_header() {
    log_message "========================================="
    log_message "FFmpeg並列エンコード処理開始"
    log_message "========================================="
    log_message "入力: $INPUT_PATH"
    log_message "作業ディレクトリ: $WORK_DIR"
    log_message "並列ジョブ数: $PARALLEL_JOBS"
    [ -n "$CRF" ] && log_message "CRF: $CRF"
    [ -n "$PRESET" ] && log_message "プリセット: $PRESET"
    [ -n "$KEYINT" ] && log_message "キーフレーム間隔: $KEYINT"
    log_message "========================================="
}

# S3ファイルの取得
get_input_file() {
    # ファイル存在チェック
    if [ -f "$LOCAL_INPUT_FILE" ]; then
        log_message "既存のファイルを再利用"
        return 0
    fi
    
    # S3からダウンロード
    log_message "S3ダウンロード開始: $INPUT_PATH"
    
    if ! aws s3 cp "$INPUT_PATH" "$LOCAL_INPUT_FILE"; then
        handle_error "S3からのダウンロードに失敗しました"
    fi
    
    log_message "S3ダウンロード完了"
}

# 動画の長さを取得する関数
get_duration() {
    local input_file="$1"
    local ffprobe_output=$(ffprobe -v quiet -print_format json -show_format "$input_file" 2>/dev/null)
    
    if [ -z "$ffprobe_output" ]; then
        handle_error "ffprobeコマンドが失敗しました"
    fi
    
    # JSONから durationを抽出
    local duration=$(echo "$ffprobe_output" | grep -o '"duration":"[^"]*"' | cut -d'"' -f4)
    if [ -z "$duration" ]; then
        duration=$(echo "$ffprobe_output" | grep -o '"duration":[^,}]*' | cut -d':' -f2 | tr -d ' "')
    fi
    
    duration=$(echo "$duration" | tr -d ' "')
    
    if [ -z "$duration" ]; then
        handle_error "動画の長さを正しく取得できませんでした"
    fi
    
    echo "$duration"
}

# タイムコードをフォーマットする関数（HH:MM:SS形式）
format_timecode() {
    local seconds="$1"
    local hours=$((seconds / 3600))
    local minutes=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))
    printf "%02d:%02d:%02d" "$hours" "$minutes" "$secs"
}

# エンコード関数
encode_segment() {
    local segment_idx="$1"
    local input_file="$2"
    local segments_dir="$3"
    local logs_dir="$4"
    local start_time="$5"
    local segment_duration="$6"
    local total_duration="$7"
    local crf="$8"
    local preset="$9"
    local keyint="${10}"
    
    # ファイル名を0埋め4桁にする
    local segment_file="$segments_dir/segment_$(printf "%04d" "$segment_idx").mp4"
    local log_file="$logs_dir/segment_$(printf "%04d" "$segment_idx").log"
    
    # タイムコードを作成
    local start_tc=$(format_timecode "$start_time")
    local end_time=$((start_time + segment_duration))
    
    # 最終セグメントの長さ調整
    local actual_duration="$segment_duration"
    if (( $(echo "$end_time > $total_duration" | bc -l) )); then
        actual_duration=$(echo "$total_duration - $start_time" | bc -l)
    fi
    
    # 進行状況をログに記録
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] セグメント $segment_idx 開始 (${start_tc} から ${actual_duration}秒)" >> "$log_file"
    
    # コマンドを配列で構築（Input Seekingで高速化）
    cmd=(ffmpeg -ss "$start_time" -i "$input_file")
    
    # 長さを指定
    cmd+=(-t "$actual_duration")
    
    # エンコードオプションを追加
    cmd+=(-c:v libsvtav1)
    
    # 指定された場合のみオプションを追加
    [ -n "$crf" ] && cmd+=(-crf "$crf")
    [ -n "$preset" ] && cmd+=(-preset "$preset")
    [ -n "$keyint" ] && cmd+=(-g "$keyint" -keyint_min "$keyint")
    
    cmd+=(-an -y "$segment_file")
    
    # 実行
    echo "エンコード中: セグメント $(printf "%04d" "$segment_idx") [${start_tc}]"
    "${cmd[@]}" >> "$log_file" 2>&1
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] セグメント $segment_idx 完了" >> "$log_file"
        echo "完了: セグメント $(printf "%04d" "$segment_idx")"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] セグメント $segment_idx エラー (終了コード: $exit_code)" >> "$log_file"
        echo "エラー: セグメント $(printf "%04d" "$segment_idx") - ログを確認してください: $log_file" >&2
    fi
    
    return $exit_code
}

# セグメント分割とエンコード処理
encode_segments() {
    log_message "ステップ1: セグメント分割・エンコード開始"
    
    # 動画の長さを取得
    DURATION=$(get_duration "$LOCAL_INPUT_FILE") || exit 1
    
    # セグメント数を計算（切り上げ）
    NUM_SEGMENTS=$(echo "($DURATION + $SEGMENT_LENGTH - 1) / $SEGMENT_LENGTH" | bc)
    
    log_message "動画の長さ: ${DURATION}秒 ($(format_timecode "${DURATION%.*}"))"
    log_message "セグメント数: $NUM_SEGMENTS (各${SEGMENT_LENGTH}秒)"
    
    # 関数をエクスポート
    export -f encode_segment
    export -f format_timecode
    
    # セグメントごとの開始時間を生成してパラレル実行
    log_message "エンコード開始 (並列数: $PARALLEL_JOBS)"
    
    # パラレル実行用のジョブリスト作成
    for i in $(seq 0 $((NUM_SEGMENTS - 1))); do
        start_time=$((i * SEGMENT_LENGTH))
        echo "$i $LOCAL_INPUT_FILE $SEGMENTS_DIR $LOGS_DIR $start_time $SEGMENT_LENGTH $DURATION $CRF $PRESET $KEYINT"
    done | parallel \
        --jobs "$PARALLEL_JOBS" \
        --halt soon,fail=1 \
        --colsep ' ' \
        encode_segment {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}
    
    if [ $? -ne 0 ]; then
        handle_error "エンコード処理に失敗"
    fi
    
    # 結果確認
    check_segment_results
    
    log_message "ステップ1: セグメントエンコード完了"
}

# セグメント結果をチェック
check_segment_results() {
    local failed=0
    
    log_message "セグメント結果確認中..."
    
    for i in $(seq 0 $((NUM_SEGMENTS - 1))); do
        local segment_file="$SEGMENTS_DIR/segment_$(printf "%04d" "$i").mp4"
        if [ ! -f "$segment_file" ]; then
            log_message "エラー: セグメント $(printf "%04d" "$i") が作成されていません"
            failed=1
        fi
    done
    
    if [ $failed -ne 0 ]; then
        handle_error "一部のセグメントが作成されていません"
    fi
    
    # 出力ファイルのサイズ情報を表示
    local total_size=$(du -sh "$SEGMENTS_DIR" | cut -f1)
    log_message "セグメント合計サイズ: $total_size"
}

# セグメント結合処理
concat_segments() {
    log_message "ステップ2: セグメント結合開始"
    
    # セグメント数をカウント
    SEGMENT_COUNT=$(ls -1 "$SEGMENTS_DIR"/segment_*.mp4 2>/dev/null | wc -l)
    
    if [ $SEGMENT_COUNT -eq 0 ]; then
        handle_error "セグメントファイルが見つかりません"
    fi
    
    log_message "結合するセグメント数: $SEGMENT_COUNT"
    
    # concat.txtを作成
    CONCAT_FILE="$WORK_DIR/concat.txt"
    > "$CONCAT_FILE"
    
    WORK_DIR_ABS=$(cd "$WORK_DIR" && pwd)
    
    for i in $(seq 0 $((SEGMENT_COUNT - 1))); do
        SEGMENT_FILE_ABS="$WORK_DIR_ABS/segments/segment_$(printf "%04d" "$i").mp4"
        echo "file '$SEGMENT_FILE_ABS'" >> "$CONCAT_FILE"
    done
    
    # ビデオのみを結合
    VIDEO_TEMP="$WORK_DIR/video_only.mp4"
    CONCAT_FILE_ABS="$WORK_DIR_ABS/concat.txt"
    
    log_message "ビデオストリーム結合中..."
    ffmpeg -f concat -safe 0 -i "$CONCAT_FILE_ABS" -c copy "$VIDEO_TEMP" 2>/dev/null
    
    if [ $? -ne 0 ]; then
        handle_error "ビデオの結合に失敗しました"
    fi
    
    # 入力ファイルの音声情報を取得
    AUDIO_COUNT=$(ffprobe -v quiet -select_streams a -show_entries stream=codec_type -of csv=p=0 "$LOCAL_INPUT_FILE" 2>/dev/null | wc -l)
    
    if [ $AUDIO_COUNT -eq 0 ]; then
        # 音声がない場合はビデオのみを出力
        mv "$VIDEO_TEMP" "$LOCAL_OUTPUT_FILE"
        log_message "音声トラックなし - ビデオのみを出力"
    else
        # 音声を抽出
        AUDIO_FILE="$WORK_DIR/audio_extracted.m4a"
        log_message "音声トラック抽出中..."
        ffmpeg -i "$LOCAL_INPUT_FILE" -vn -c:a aac "$AUDIO_FILE" 2>/dev/null
        
        if [ $? -ne 0 ]; then
            handle_error "音声の抽出に失敗しました"
        fi
        
        # ビデオと音声を結合
        log_message "ビデオと音声を多重化中..."
        ffmpeg -i "$VIDEO_TEMP" -i "$AUDIO_FILE" -c:v copy -c:a copy -map 0:v:0 -map 1:a "$LOCAL_OUTPUT_FILE" 2>/dev/null
        
        if [ $? -ne 0 ]; then
            handle_error "ビデオと音声の結合に失敗しました"
        fi
        
        # 一時ファイルをクリーンアップ
        rm -f "$VIDEO_TEMP" "$AUDIO_FILE"
        log_message "音声トラック結合完了"
    fi
    
    log_message "ステップ2: 結合処理完了"
}

# S3へのディレクトリ同期
sync_to_s3() {
    local source="$1"
    local destination="$2"
    local description="$3"
    
    log_message "${description}を同期中..."
    if aws s3 sync "$source" "$destination"; then
        log_message "${description}同期成功: $destination"
        return 0
    else
        log_message "${description}同期失敗"
        return 1
    fi
}

# S3アップロード処理
process_s3_upload() {
    log_message "ステップ3: S3アップロード開始"
    
    # 出力パスの設定
    S3_OUTPUT_PATH="s3://$S3_BUCKET/output"
    
    # 作業ディレクトリの同期（ログ含む）
    sync_to_s3 "$WORK_DIR" "$S3_OUTPUT_PATH/$WORK_DIR/" "作業ディレクトリ（ログ含む）"
    
    log_message "ステップ3: S3アップロード完了"
}

# 完了メッセージの表示
print_completion() {
    local end_time=$(date '+%Y-%m-%d %H:%M:%S')
    local elapsed_time=$(($(date +%s) - START_TIME))
    local elapsed_min=$((elapsed_time / 60))
    local elapsed_sec=$((elapsed_time % 60))
    
    log_message "========================================="
    log_message "全処理完了"
    log_message "処理時間: ${elapsed_min}分${elapsed_sec}秒"
    log_message "出力先: $S3_OUTPUT_PATH/$WORK_DIR/"
    log_message "========================================="
}

# ========================================
# メイン処理
# ========================================

# 開始時刻を記録
START_TIME=$(date +%s)

# パラメータの取得
INPUT_FILENAME="$1"
PARALLEL_JOBS="${2:-4}"
CRF="$3"
PRESET="$4"
KEYINT="$5"

# S3パスの構築
INPUT_PATH="s3://$S3_BUCKET/input/$INPUT_FILENAME"

# スクリプトディレクトリの取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# パラメータ検証
validate_parameters

# 作業環境の初期化
initialize_workspace

# ヘッダー出力
print_header

# 入力ファイルの取得
get_input_file

# ステップ1: 分割・エンコード
encode_segments

# ステップ2: 結合
concat_segments

# ステップ3: S3へのアップロード
process_s3_upload

# 完了メッセージ
print_completion
