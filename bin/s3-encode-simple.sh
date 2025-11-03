#!/bin/bash
# シンプルなS3エンコーディングスクリプト
# 使用法: ./s3-encode-simple.sh <s3-bucket> <video>

set -euo pipefail

# 引数チェック
if [[ $# -lt 2 ]]; then
    echo "使用法: $0 <s3-bucket> <video> [parallel]" >&2
    echo "例: $0 my-bucket video.mp4 4" >&2
    exit 1
fi

BUCKET="$1"
VIDEO="$2"
PARALLEL="${3:-4}"  # デフォルト4並列

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENCODER="${PROJECT_ROOT}/unified_parallel_encode.py"

echo "=========================================="
echo "S3動画エンコーディング"
echo "=========================================="
echo "バケット: $BUCKET"
echo "動画: $VIDEO"
echo "並列数: $PARALLEL"
echo "=========================================="

# 作業ディレクトリを作成
VIDEO_BASE="${VIDEO%.*}"
WORKSPACE="encode_${VIDEO_BASE}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$WORKSPACE"
echo "作業ディレクトリ: $WORKSPACE"

# ダウンロード
echo "S3からダウンロード中..."
aws s3 cp "s3://${BUCKET}/input/${VIDEO}" "./${VIDEO}"

# エンコード
echo "エンコード中..."
"$ENCODER" "$VIDEO" --workspace "$WORKSPACE" --parallel "$PARALLEL" -- -crf 30 -preset 6

# アップロード
echo "S3へアップロード中..."
aws s3 cp "${WORKSPACE}/output.mkv" "s3://${BUCKET}/output/${VIDEO}"

# クリーンアップ
echo "クリーンアップ中..."
rm -f "$VIDEO"
rm -rf "$WORKSPACE"

echo "=========================================="
echo "処理完了"
echo "=========================================="
