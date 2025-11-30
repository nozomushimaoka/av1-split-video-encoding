# AV1 並列動画エンコーディング

動画を複数のセグメントに分割し、並列でAV1エンコードを行うツールです。

## 特徴

- セグメント分割による並列エンコーディング
- FFmpegとSvtAv1EncAppのパイプライン処理
- S3連携によるバッチ処理
- 未処理ファイルの自動検出
- 音声トラックの自動処理（コピーまたは再エンコード）

## 必要な環境

- Python 3.10以上
- FFmpeg
- SvtAv1EncApp（PATHに含まれている必要があります）
- AWS CLI（S3を使用する場合）

## セットアップ

### 依存関係のインストール

```bash
pip install -r requirements.txt
```

### AWS認証情報の設定（S3を使用する場合）

```bash
aws configure
```

## 使い方

### ローカルファイルのエンコード

```bash
python -m av1_encoder.encoding input.mkv workspace/ \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6
```

#### オプション

| オプション | 短縮形 | 説明 |
|------------|--------|------|
| `input_file` | - | 入力ファイルパス（必須） |
| `workspace` | - | 作業ディレクトリパス（必須） |
| `--parallel` | `-l` | 並列ジョブ数（必須） |
| `--gop` | `-g` | GOPサイズ（キーフレーム間隔、必須） |
| `--svtav1-params` | - | SvtAv1EncAppのパラメータ（カンマ区切り、必須） |
| `--ffmpeg-params` | - | FFmpegのパラメータ（カンマ区切り、オプション） |
| `--verbose` | `-v` | 詳細なログを出力（DEBUGレベル） |

#### パラメータの指定方法

`--svtav1-params`と`--ffmpeg-params`はカンマ区切りで指定します：

```bash
# 例: crf=30, preset=6を指定
--svtav1-params crf=30,preset=6

# 展開後: --crf 30 --preset 6
```

### S3バッチエンコード

S3バケットからファイルをダウンロードし、エンコード後にアップロードします。

```bash
python -m av1_encoder.s3 \
    --bucket my-bucket \
    --pending-files pending.txt \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6
```

#### オプション

| オプション | 短縮形 | 説明 |
|------------|--------|------|
| `--bucket` | - | S3バケット名（環境変数`S3_BUCKET`でも指定可） |
| `--pending-files` | - | 処理対象ファイルリストのパス（必須） |
| `--parallel` | `-l` | 並列ジョブ数（必須） |
| `--gop` | `-g` | GOPサイズ（必須） |
| `--svtav1-params` | - | SvtAv1EncAppのパラメータ（必須） |
| `--ffmpeg-params` | - | FFmpegのパラメータ（オプション） |
| `--audio-params` | - | 音声エンコードのパラメータ（オプション） |
| `--verbose` | `-v` | 詳細なログを出力（DEBUGレベル） |

#### S3バケットの構造

```
my-bucket/
├── input/           # 入力ファイル
│   ├── video1.mkv
│   └── subdir/video2.mkv
└── output/          # 出力ファイル
    ├── video1.mkv
    └── subdir/video2.mkv
```

#### 音声パラメータの指定

音声を再エンコードする場合は`--audio-params`を指定します：

```bash
--audio-params c:a=aac,b:a=128k

# 展開後: -c:a aac -b:a 128k
```

指定しない場合、音声はコピーされます。

### 未処理ファイルの一覧取得

S3バケット内で未処理のファイルを一覧表示します。

```bash
python -m av1_encoder.list_pending --bucket my-bucket

# ファイルに出力
python -m av1_encoder.list_pending --bucket my-bucket > pending.txt

# 詳細ログを表示
python -m av1_encoder.list_pending --bucket my-bucket --verbose
```

## 処理フロー

1. **セグメント分割**: 動画を60秒ごとに分割（GOP境界に整列）
2. **並列エンコード**: 各セグメントをFFmpegでデコード → SvtAv1EncAppでAV1エンコード
3. **セグメント結合**: FFmpegでエンコード済みセグメントを結合
4. **音声処理**: 元動画から音声を抽出し、エンコード済み動画と多重化

## 出力

処理結果は作業ディレクトリに保存されます：

```
workspace/
├── output.mkv           # 最終出力ファイル
├── main.log             # 全体のログ
├── concat.txt           # セグメント結合用リスト
├── segment_0000.log     # 各セグメントのエンコードログ
├── segment_0001.log
└── ...
```

セグメントファイル（`.ivf`）は処理完了後に削除されます。

## プロジェクト構造

```
av1-split-video-encoding/
├── av1_encoder/
│   ├── core/                 # コア機能
│   │   ├── config.py         # 設定データクラス
│   │   ├── workspace.py      # ワークスペース管理
│   │   ├── logging_config.py # ログ設定
│   │   ├── video_probe.py    # 動画情報取得
│   │   ├── command_builder.py # コマンド構築
│   │   └── ffmpeg.py         # FFmpegサービス
│   ├── encoding/             # ローカルエンコード
│   │   ├── cli.py            # CLIエントリーポイント
│   │   └── encoder.py        # エンコードオーケストレーター
│   ├── s3/                   # S3連携
│   │   ├── cli.py            # CLIエントリーポイント
│   │   ├── pipeline.py       # S3パイプライン
│   │   ├── batch_orchestrator.py # バッチ処理
│   │   ├── file_processor.py # ファイル処理
│   │   └── video_merger.py   # 動画結合
│   ├── list_pending/         # 未処理ファイル検出
│   │   ├── cli.py            # CLIエントリーポイント
│   │   └── pending.py        # 未処理ファイル計算
│   └── cli_utils.py          # CLI共通ユーティリティ
├── tests/                    # テストスイート
├── requirements.txt          # 依存関係
└── README.md
```

## トラブルシューティング

### SvtAv1EncAppが見つからない

```bash
which SvtAv1EncApp
```

PATHに含まれていることを確認してください。SVT-AV1をソースからビルドし、インストールする必要があります。

### S3アクセスエラー

AWS認証情報とS3バケットへのアクセス権限を確認してください。

```bash
aws s3 ls s3://my-bucket/
```

### メモリ不足

並列数を減らしてください：

```bash
python -m av1_encoder.encoding input.mkv workspace/ \
    --parallel 2 --gop 240 --svtav1-params crf=30
```

## ライセンス

MIT License
