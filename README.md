# AV1 並列動画エンコーディング

動画を複数のセグメントに分割し、並列でAV1エンコードを行うツールです。

## 特徴

- セグメント分割による並列エンコーディング
- 音声トラックの自動処理
- 型ヒント付きPythonコード
- 詳細なログ出力
- AWS S3との連携（AWS CLIを使用）

## 必要な環境

- Python 3.8以上
- FFmpeg (libsvtav1サポート)
- AWS CLI (S3を使用する場合)

## セットアップ

### 1. 依存関係のインストール

```bash
pip install -r requirements.txt
```

### 2. AWS認証情報の設定

```bash
aws configure
```

## 使い方

### ローカルファイルのエンコード

```bash
./unified_parallel_encode.py video.mkv
```

### オプション付き実行

```bash
./unified_parallel_encode.py video.mkv --parallel 8 -- -crf 30 -preset 6 -pix_fmt yuv420p10le
```

### S3との連携（ダウンロード → エンコード → アップロード）

```bash
# S3からダウンロード
aws s3 cp s3://my-bucket/input/video.mkv ./video.mkv

# エンコード
./unified_parallel_encode.py video.mkv --parallel 8 -- -crf 30 -preset 6

# S3へアップロード
aws s3 cp encode_video_*/output.mkv s3://my-bucket/output/video.mkv

# クリーンアップ
rm -rf video.mkv encode_video_*
```

### パイプライン化（複数動画を並行処理）

```bash
#!/bin/bash
# 複数動画を並行処理（エンコード中にアップロード）

for video in video1.mp4 video2.mp4 video3.mp4; do
  (
    # ダウンロード
    aws s3 cp "s3://my-bucket/input/$video" "./$video"

    # エンコード
    ./unified_parallel_encode.py "$video" --parallel 4 -- -crf 30 -preset 6

    # アップロード（前のエンコードと並行実行される）
    aws s3 cp "encode_${video%.*}_"*/output.mkv "s3://my-bucket/output/$video"

    # クリーンアップ
    rm -f "$video"
    rm -rf "encode_${video%.*}_"*
  ) &
done

wait  # すべてのジョブ完了を待つ
```

### コマンドライン引数

- `input_file`: 入力ファイルパス（ローカルファイル）
- `--parallel, -l`: 並列ジョブ数（デフォルト: 4）
- `extra_args`: `--` の後に追加のFFmpegオプション
  - 例: `-crf 30 -preset 6 -pix_fmt yuv420p10le -g 240 -keyint_min 240`

### ヘルプの表示

```bash
./unified_parallel_encode.py --help
```

## 処理フロー

1. **セグメント分割・エンコード**: 動画を60秒ごとに分割し、並列でAV1エンコード
2. **セグメント結合**: エンコード済みセグメントを結合
3. **音声処理**: 元動画から音声を抽出し、エンコード済み動画と多重化

## 出力

処理結果は以下のディレクトリに保存されます：

```
encode_<動画名>_<タイムスタンプ>/
├── output.mkv           # 最終出力ファイル
├── segments/            # エンコード済みセグメント（処理後に削除）
│   ├── segment_0000.mp4
│   ├── segment_0001.mp4
│   └── ...
├── logs/                # 各セグメントのエンコードログ
│   ├── main.log         # 全体のログ
│   ├── segment_0000.log
│   ├── segment_0001.log
│   └── ...
└── concat.txt           # セグメント結合用リスト
```

## プロジェクト構造

```
av1-split-video-encoding/
├── av1_encoder/              # メインパッケージ
│   ├── __init__.py           # パッケージ初期化
│   ├── config.py             # データクラス（設定、セグメント情報）
│   ├── workspace.py          # ワークスペース管理
│   ├── ffmpeg.py             # FFmpeg操作サービス
│   ├── encoder.py            # エンコード処理オーケストレーター
│   └── cli.py                # CLIエントリーポイント
├── tests/                    # テストスイート
│   ├── test_cli.py
│   ├── test_encoder.py
│   ├── test_ffmpeg.py
│   └── test_workspace.py
├── unified_parallel_encode.py # 実行スクリプト
├── requirements.txt          # 依存関係
└── README.md                 # このファイル
```

### モジュールの責務

- **config.py**: データモデルの定義（`EncodingConfig`, `SegmentInfo`）
- **workspace.py**: 作業ディレクトリとログの管理
- **ffmpeg.py**: FFmpegコマンド実行（動画情報取得、エンコード、結合、音声処理）
- **encoder.py**: 処理全体のオーケストレーション（並列実行管理、フロー制御）
- **cli.py**: コマンドライン引数処理とmain関数

## 設計の特徴

### UNIX哲学

- **単一責任**: エンコーダーはエンコードのみに集中
- **組み合わせ可能**: AWS CLIと組み合わせてS3連携
- **パイプライン化**: シェルスクリプトでワークフロー構築可能

### 改善点

- **型安全性**: 型ヒントによる安全性向上
- **エラーハンドリング**: Pythonの例外処理による堅牢性向上
- **可読性**: クラスと関数による構造化
- **保守性**: モジュール分割とテストの容易性
- **ログ管理**: Python loggingモジュールによる統一的なログ管理
- **責務分離**: 各モジュールが明確な責務を持つオブジェクト指向設計

## トラブルシューティング

### FFmpegがlibsvtav1をサポートしていない

```bash
ffmpeg -codecs | grep av1
```

libsvtav1が表示されない場合は、FFmpegを再ビルドする必要があります。

### S3アクセスエラー

AWS認証情報とS3バケットへのアクセス権限を確認してください。

```bash
aws s3 ls s3://my-bucket/
```

### メモリ不足

並列数を減らしてください：

```bash
./unified_parallel_encode.py video.mkv --parallel 2
```

### ディスク容量不足

複数動画の並行処理時は、動画ごとにクリーンアップすることで、一度に必要なディスク容量を削減できます。

## ライセンス

MIT License
