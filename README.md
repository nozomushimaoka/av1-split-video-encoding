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
- FFmpeg
- SvtAv1EncApp (PATH に含まれている必要があります)
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
# 作業ディレクトリを作成
mkdir workspace

# エンコード実行
./unified_parallel_encode.py video.mkv --workspace workspace
```

### オプション付き実行

```bash
mkdir workspace
# 個別オプション形式
./unified_parallel_encode.py video.mkv --workspace workspace --parallel 8 -- --crf 30 --preset 6

# -svtav1-params形式（コロン区切りで複数のパラメータを指定）
./unified_parallel_encode.py video.mkv --workspace workspace --parallel 8 -- -svtav1-params preset=4:crf=30:enable-qm=1:qm-min=8:scd=1
```

### S3との連携（bin/s3-encode-simple.shを使用）

```bash
# 単一動画のエンコード
./bin/s3-encode-simple.sh my-bucket video.mkv 4

# パイプラインスクリプトで複数動画を並行処理
./bin/s3-encode-pipeline.sh my-bucket video1.mkv video2.mkv video3.mkv
```

### 手動でのS3連携

```bash
# 作業ディレクトリを作成
WORKSPACE="encode_video_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$WORKSPACE"

# S3からダウンロード
aws s3 cp s3://my-bucket/input/video.mkv ./video.mkv

# エンコード
./unified_parallel_encode.py video.mkv --workspace "$WORKSPACE" --parallel 8 -- -crf 30 -preset 6

# S3へアップロード
aws s3 cp "${WORKSPACE}/output.mkv" s3://my-bucket/output/video.mkv

# クリーンアップ
rm -f video.mkv
rm -rf "$WORKSPACE"
```

### コマンドライン引数

- `input_file`: 入力ファイルパス（ローカルファイル）
- `--workspace, -w`: 作業ディレクトリパス（必須）
  - エンコード前に作成しておく必要があります
  - 出力ファイルは `<workspace>/output.mkv` に保存されます
- `--parallel, -l`: 並列ジョブ数（デフォルト: 4）
- `--gop, -g`: GOP サイズ（キーフレーム間隔、必須）
- `svtav1_args`: `--` の後に追加のSvtAv1EncAppオプション
  - 個別オプション形式: `--crf 30 --preset 6`
  - `-svtav1-params`形式: `-svtav1-params preset=4:crf=30:enable-qm=1:qm-min=8:scd=1`
    - コロン区切りで複数のパラメータを指定可能
    - `key=value`形式で指定したパラメータは自動的に`--key value`に展開されます

### ヘルプの表示

```bash
./unified_parallel_encode.py --help
```

## 処理フロー

1. **セグメント分割**: 動画を60秒ごとに分割
2. **並列エンコード**: 各セグメントをFFmpegでデコードし、SvtAv1EncAppでAV1エンコード（IVF形式）
3. **セグメント結合**: エンコード済みセグメントをFFmpegで結合
4. **音声処理**: 元動画から音声を抽出し、エンコード済み動画と多重化（MKV形式）

## 出力

処理結果は指定した作業ディレクトリに保存されます（フラット構造）：

```
<workspace>/
├── output.mkv           # 最終出力ファイル（MKV形式）
├── main.log             # 全体のログ
├── segment_0000.ivf     # エンコード済みセグメント（IVF形式、処理後に削除）
├── segment_0001.ivf
├── segment_0002.ivf
├── ...
├── segment_0000.log     # 各セグメントのエンコードログ
├── segment_0001.log
├── segment_0002.log
├── ...
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
- **ffmpeg.py**: FFmpegとSvtAv1EncAppの実行（動画情報取得、パイプライン処理、結合、音声処理）
  - `expand_svtav1_params()`: `-svtav1-params`形式のパラメータ展開
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

### SvtAv1EncAppが見つからない

```bash
which SvtAv1EncApp
```

SvtAv1EncAppがPATHに含まれていることを確認してください。SVT-AV1をソースからビルドし、インストールする必要があります。

### S3アクセスエラー

AWS認証情報とS3バケットへのアクセス権限を確認してください。

```bash
aws s3 ls s3://my-bucket/
```

### メモリ不足

並列数を減らしてください：

```bash
mkdir workspace
./unified_parallel_encode.py video.mkv --workspace workspace --parallel 2
```

### ディスク容量不足

複数動画の並行処理時は、動画ごとにクリーンアップすることで、一度に必要なディスク容量を削減できます。

## ライセンス

MIT License
