#!/bin/bash
# コマンド失敗で終了
set -euo pipefail

# 引数チェック
if [ $# -ne 1 ]; then
    exit 1
fi

input_file="$1"

if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET environment variable must be set" >&2
    exit 1
fi

workspace="encode_$input_file_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$workspace"

# S3からダウンロード
aws s3 cp "s3://$S3_BUCKET/input/$input_file.mkv" "$workspace/input.mkv"

# エンコード
./unified_parallel_encode.py "$workspace/input.mkv" \
    --workspace "$workspace" --parallel 10 -- \
    -crf 36 -preset 6 -pix_fmt yuv420p10le \
    -svtav1-params tune=0:enable-qm=1:qm-min=0

# 結合
ffmpeg -f concat \
    -i "$workspace/concat.txt" \
    -i "$workspace/input.mkv" \
    -map 0:v:0 -map 1:a \
    -c:v copy -c:a copy \
    "$workspace/output.mkv"

# 入力ファイルを削除
rm "$workspace/input.mkv"

# S3へアップロード
aws s3 cp "${workspace}/output.mkv" "s3://$S3_BUCKET/output/$input_file"

# 出力ファイルを削除
rm "$workspace/output.mkv"

# クリーンアップ
rm "$workspace/"*.mp4
