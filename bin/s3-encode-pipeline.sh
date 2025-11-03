#!/bin/bash
# S3から複数動画をダウンロード → エンコード → アップロードを並行実行
# 使用法: ./s3-encode-pipeline.sh <s3-bucket> <video1> <video2> ...

set -euo pipefail

# 使用方法を表示
usage() {
    cat <<EOF
使用法: $0 <s3-bucket> <video1> <video2> ...

オプション:
  -h, --help        ヘルプを表示
  -p, --parallel N  並列ジョブ数（デフォルト: 4）
  -k, --keep        一時ファイルを削除しない

例:
  $0 my-bucket video1.mp4 video2.mp4 video3.mp4
  $0 -p 8 my-bucket video1.mp4 video2.mp4
EOF
    exit 0
}

# デフォルト設定
PARALLEL_JOBS=4
KEEP_FILES=false
BUCKET=""
VIDEOS=()

# 引数解析
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            ;;
        -p|--parallel)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        -k|--keep)
            KEEP_FILES=true
            shift
            ;;
        *)
            if [[ -z "$BUCKET" ]]; then
                BUCKET="$1"
            else
                VIDEOS+=("$1")
            fi
            shift
            ;;
    esac
done

# バケット名と動画リストのチェック
if [[ -z "$BUCKET" ]]; then
    echo "エラー: S3バケット名を指定してください" >&2
    usage
fi

if [[ ${#VIDEOS[@]} -eq 0 ]]; then
    echo "エラー: 少なくとも1つの動画を指定してください" >&2
    usage
fi

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENCODER="${PROJECT_ROOT}/unified_parallel_encode.py"

# エンコーダーの存在確認
if [[ ! -x "$ENCODER" ]]; then
    echo "エラー: エンコーダーが見つかりません: $ENCODER" >&2
    exit 1
fi

# AWS CLIの確認
if ! command -v aws &> /dev/null; then
    echo "エラー: AWS CLIがインストールされていません" >&2
    exit 1
fi

# 作業ディレクトリの作成
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

echo "=========================================="
echo "S3エンコーディングパイプライン"
echo "=========================================="
echo "バケット: $BUCKET"
echo "並列ジョブ数: $PARALLEL_JOBS"
echo "動画数: ${#VIDEOS[@]}"
echo "作業ディレクトリ: $WORK_DIR"
echo "=========================================="

# 各動画を並行処理
for video in "${VIDEOS[@]}"; do
    (
        set -euo pipefail

        video_name="$(basename "$video")"
        video_base="${video_name%.*}"
        local_input="${WORK_DIR}/${video_name}"

        # 動画ごとの作業ディレクトリを作成
        workspace="${WORK_DIR}/encode_${video_base}_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$workspace"

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] 処理開始"

        # ダウンロード
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] S3からダウンロード中..."
        if ! aws s3 cp "s3://${BUCKET}/input/${video_name}" "$local_input"; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] エラー: ダウンロード失敗" >&2
            exit 1
        fi

        # エンコード
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] エンコード中..."
        if ! "$ENCODER" "$local_input" --workspace "$workspace" --parallel "$PARALLEL_JOBS" -- -crf 30 -preset 6; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] エラー: エンコード失敗" >&2
            rm -f "$local_input"
            rm -rf "$workspace"
            exit 1
        fi

        # 出力ファイルの確認
        output_file="${workspace}/output.mkv"
        if [[ ! -f "$output_file" ]]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] エラー: 出力ファイルが見つかりません" >&2
            rm -f "$local_input"
            rm -rf "$workspace"
            exit 1
        fi

        # アップロード
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] S3へアップロード中..."
        if ! aws s3 cp "$output_file" "s3://${BUCKET}/output/${video_name}"; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] エラー: アップロード失敗" >&2
            rm -f "$local_input"
            rm -rf "$workspace"
            exit 1
        fi

        # クリーンアップ
        if [[ "$KEEP_FILES" == "false" ]]; then
            rm -f "$local_input"
            rm -rf "$workspace"
        fi

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$video_name] 処理完了"
    ) &
done

# すべてのジョブの完了を待つ
echo ""
echo "すべてのジョブを実行中..."
wait

echo ""
echo "=========================================="
echo "すべての処理が完了しました"
echo "=========================================="
