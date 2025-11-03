#!/bin/bash
# コマンド失敗で終了
set -euo pipefail

if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET environment variable must be set" >&2
    exit 1
fi

echo "S3バケット: $S3_BUCKET"

# S3のinput/から.mkvファイル一覧を取得
echo "input/内の.mkvファイルを取得中..."
input_files=$(aws s3 ls "s3://$S3_BUCKET/input/" | grep '\.mkv$' | awk '{print $4}')

if [ -z "$input_files" ]; then
    echo "Error: No .mkv files found in s3://$S3_BUCKET/input/" >&2
    exit 1
fi

# output/の既存ファイル一覧を取得
echo "output/内の既存ファイルを取得中..."
output_files=$(aws s3 ls "s3://$S3_BUCKET/output/" | awk '{print $4}')

# 差分を計算（commコマンドを使用）
# input_filesから.mkvを除いたリストと、output_filesの差分を取得
input_base_names=$(echo "$input_files" | sed 's/\.mkv$//' | sort)
output_base_names=$(echo "$output_files" | sort)

# commで差分を取得: input_base_namesにのみ存在するもの
todo_base_names=$(comm -23 <(echo "$input_base_names") <(echo "$output_base_names"))

# 未処理ファイルのリストを作成
mapfile -t todo_files < <(echo "$todo_base_names" | sed 's/$/.mkv/')

# 処理対象を表示
if [ -n "$output_files" ]; then
    while IFS= read -r file; do
        echo "スキップ: ${file}.mkv (既に処理済み)"
    done < <(echo "$output_base_names")
fi

while IFS= read -r file; do
    [ -n "$file" ] && echo "処理対象: ${file}.mkv"
done < <(echo "$todo_base_names")

# 処理対象がない場合
if [ ${#todo_files[@]} -eq 0 ]; then
    echo "すべてのファイルが処理済みです"
    exit 0
fi

echo ""
echo "処理開始: ${#todo_files[@]}ファイル"
echo ""

# ダウンロード関数
download_file() {
    local file=$1
    if [ -f "$file" ]; then
        echo "[DL] スキップ: $file は既に存在します"
        return 0
    fi
    echo "[DL] ダウンロード開始: $file"
    aws s3 cp "s3://$S3_BUCKET/input/$file" "$file"
    echo "[DL] ダウンロード完了: $file"
}

# バックグラウンドでのダウンロードPID管理
download_pid=""

# 各ファイルを処理
for i in "${!todo_files[@]}"; do
    input_file="${todo_files[$i]}"

    echo "========================================="
    echo "処理中 ($((i+1))/${#todo_files[@]}): $input_file"
    echo "========================================="

    # 現在のファイルのダウンロード待ち
    if [ -n "$download_pid" ]; then
        wait "$download_pid"
        download_pid=""
    fi

    # 次のファイルのダウンロードをバックグラウンドで開始
    next_index=$((i+1))
    if [ $next_index -lt ${#todo_files[@]} ]; then
        next_file="${todo_files[$next_index]}"
        download_file "$next_file" &
        download_pid=$!
        echo "[DL] 次ファイルのダウンロード開始: $next_file (PID: $download_pid)"
    fi

    # 現在のファイルが存在しない場合はダウンロード
    if [ ! -f "$input_file" ]; then
        echo "エラー: $input_file が見つかりません"
        exit 1
    fi

    # 拡張子を除いたベース名
    base_name="${input_file%.mkv}"

    workspace="encode_${base_name}_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$workspace"

    # エンコード
    echo "エンコード中..."
    ./unified_parallel_encode.py "$input_file" \
        --workspace "$workspace" --parallel 10 \
        -- -crf 36 -preset 6 -pix_fmt yuv420p10le \
        -svtav1-params tune=0:enable-qm=1:qm-min=0

    # 結合
    echo "結合中..."
    ffmpeg -f concat \
        -i "$workspace/concat.txt" \
        -i "$input_file" \
        -map 0:v:0 -map 1:a \
        -c:v copy -c:a copy \
        "$workspace/output.mkv"

    # 入力ファイルを削除
    rm "$input_file"

    # S3へアップロード（バックグラウンド）
    echo "アップロード中..."
    aws s3 cp "${workspace}/output.mkv" "s3://$S3_BUCKET/output/$base_name" &
    upload_pid=$!

    # 出力ファイルを削除（アップロード完了後）
    (
        wait "$upload_pid"
        rm "$workspace/output.mkv"
        rm "$workspace/"*.mp4
        echo "完了: $input_file"
    ) &

    echo "次の処理に移行（アップロードは継続中）"
done

# 最後のダウンロードとアップロードを待つ
if [ -n "$download_pid" ]; then
    wait "$download_pid"
fi

# すべてのバックグラウンドジョブの完了を待つ
wait

echo ""
echo "すべての処理が完了しました"
