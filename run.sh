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

# 各ファイルを処理
for input_file in "${todo_files[@]}"; do
    echo "========================================="
    echo "処理中: $input_file"
    echo "========================================="

    # 拡張子を除いたベース名
    base_name="${input_file%.mkv}"

    workspace="encode_${base_name}_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$workspace"

    # S3からダウンロード
    if [ -f "$input_file" ]; then
        echo "スキップ: $input_file は既に存在します"
    else
        echo "ダウンロード中..."
        aws s3 cp "s3://$S3_BUCKET/input/$input_file" "$input_file"
    fi

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
        -i "$workspace/input.mkv" \
        -map 0:v:0 -map 1:a \
        -c:v copy -c:a copy \
        "$workspace/output.mkv"

    # 入力ファイルを削除
    rm "$input_file"

    # S3へアップロード
    echo "アップロード中..."
    aws s3 cp "${workspace}/output.mkv" "s3://$S3_BUCKET/output/$input_file"

    # 出力ファイルを削除
    rm "$workspace/output.mkv"

    # クリーンアップ
    rm "$workspace/"*.mp4

    echo "完了: $input_file"
done

echo "すべての処理が完了しました"
