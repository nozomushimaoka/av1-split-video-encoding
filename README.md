# AV1 並列動画エンコーディング

動画を複数のセグメントに分割し、並列でAV1エンコードを行うツールです。

## 特徴

- S3からの入力ファイル取得と結果のアップロード
- セグメント分割による並列エンコーディング
- 音声トラックの自動処理
- 型ヒント付きPythonコード
- 詳細なログ出力

## 必要な環境

- Python 3.8以上
- FFmpeg (libsvtav1サポート)
- AWS CLI (S3アクセス用)

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

### 基本的な使い方

```bash
./unified_parallel_encode.py video.mkv
```

### オプション付き実行

```bash
./unified_parallel_encode.py video.mkv --parallel 8 --crf 23 --preset medium --keyint 240
```

### コマンドライン引数

- `input_filename`: 入力ファイル名（S3バケットのinput/内）
- `--parallel, -p`: 並列ジョブ数（デフォルト: 4）
- `--crf`: Constant Rate Factor（品質設定）
- `--preset`: エンコード速度プリセット（ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow）
- `--keyint`: キーフレーム間隔
- `--bucket`: S3バケット名（デフォルト: xxx）

### ヘルプの表示

```bash
./unified_parallel_encode.py --help
```

## 処理フロー

1. **S3からダウンロード**: 入力動画をS3から取得
2. **セグメント分割・エンコード**: 動画を60秒ごとに分割し、並列でAV1エンコード
3. **セグメント結合**: エンコード済みセグメントを結合
4. **音声処理**: 元動画から音声を抽出し、エンコード済み動画と多重化
5. **S3へアップロード**: 結果とログをS3にアップロード

## 出力

処理結果は以下のディレクトリに保存されます：

```
encode_<動画名>_<タイムスタンプ>/
├── input_video          # ダウンロードした入力ファイル
├── <出力ファイル名>     # 最終出力ファイル
├── segments/            # エンコード済みセグメント
│   ├── segment_0000.mp4
│   ├── segment_0001.mp4
│   └── ...
├── logs/                # 各セグメントのエンコードログ
│   ├── segment_0000.log
│   ├── segment_0001.log
│   └── ...
├── concat.txt           # セグメント結合用リスト
└── encode.log           # 全体のログ
```

## プロジェクト構造

```
av1-split-video-encoding/
├── av1_encoder/              # メインパッケージ
│   ├── __init__.py           # パッケージ初期化
│   ├── config.py             # データクラス（設定、セグメント情報）
│   ├── workspace.py          # ワークスペース管理
│   ├── ffmpeg.py             # FFmpeg操作サービス
│   ├── storage.py            # S3操作サービス
│   ├── encoder.py            # エンコード処理オーケストレーター
│   └── cli.py                # CLIエントリーポイント
├── unified_parallel_encode.py # 実行スクリプト
├── requirements.txt          # 依存関係
└── README.md                 # このファイル
```

### モジュールの責務

- **config.py**: データモデルの定義（`EncodingConfig`, `WorkspaceConfig`, `SegmentInfo`）
- **workspace.py**: 作業ディレクトリとログの管理
- **ffmpeg.py**: FFmpegコマンド実行（動画情報取得、エンコード、結合、音声処理）
- **storage.py**: S3操作（ダウンロード、アップロード）
- **encoder.py**: 処理全体のオーケストレーション（並列実行管理、フロー制御）
- **cli.py**: コマンドライン引数処理とmain関数

## 旧シェルスクリプトからの変更点

### 改善点

- **型安全性**: 型ヒントによる安全性向上
- **エラーハンドリング**: Pythonの例外処理による堅牢性向上
- **可読性**: クラスと関数による構造化
- **保守性**: モジュール分割とテストの容易性
- **ログ管理**: Python loggingモジュールによる統一的なログ管理
- **責務分離**: 各モジュールが明確な責務を持つオブジェクト指向設計

### 互換性

旧シェルスクリプト（`unified_parallel_encode.sh`）と同等の機能を提供します：

```bash
# 旧シェルスクリプト
./unified_parallel_encode.sh video.mkv 8 23 medium 240

# 新Pythonスクリプト
./unified_parallel_encode.py video.mkv --parallel 8 --crf 23 --preset medium --keyint 240
```

## トラブルシューティング

### FFmpegがlibsvtav1をサポートしていない

```bash
ffmpeg -codecs | grep av1
```

libsvtav1が表示されない場合は、FFmpegを再ビルドする必要があります。

### S3アクセスエラー

AWS認証情報とS3バケットへのアクセス権限を確認してください。

```bash
aws s3 ls s3://xxx/
```

### メモリ不足

並列数を減らしてください：

```bash
./unified_parallel_encode.py video.mkv --parallel 2
```

## ライセンス

MIT License
