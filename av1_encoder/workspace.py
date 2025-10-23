"""ワークスペース管理"""

import logging
from datetime import datetime
from pathlib import Path

from .config import WorkspaceConfig


class Workspace:
    """作業ディレクトリ管理クラス"""

    def __init__(self, input_filename: str):
        """
        Args:
            input_filename: 入力ファイル名
        """
        self.input_filename = input_filename
        self.config = self._initialize()

    def _initialize(self) -> WorkspaceConfig:
        """作業ディレクトリを初期化"""
        # ファイル名から作業ディレクトリ名を生成
        input_basename = Path(self.input_filename).stem
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        work_dir = Path(f"encode_{input_basename}_{timestamp}")
        work_dir.mkdir(exist_ok=True)

        # サブディレクトリ作成
        segments_dir = work_dir / "segments"
        logs_dir = work_dir / "logs"
        segments_dir.mkdir(exist_ok=True)
        logs_dir.mkdir(exist_ok=True)

        return WorkspaceConfig(
            work_dir=work_dir,
            segments_dir=segments_dir,
            logs_dir=logs_dir,
            local_input_file=work_dir / "input_video",
            local_output_file=work_dir / self.input_filename,
            concat_file=work_dir / "concat.txt",
            log_file=work_dir / "encode.log"
        )

    def setup_logging(self) -> logging.Logger:
        """ログ設定を初期化"""
        logger = logging.getLogger("av1_encoder")
        logger.setLevel(logging.INFO)

        # 既存のハンドラをクリア
        logger.handlers.clear()

        # ファイルハンドラ
        file_handler = logging.FileHandler(self.config.log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        # コンソールハンドラ
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # フォーマッター
        formatter = logging.Formatter(
            '[%(asctime)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    @property
    def work_dir(self) -> Path:
        """作業ディレクトリパス"""
        return self.config.work_dir

    @property
    def segments_dir(self) -> Path:
        """セグメントディレクトリパス"""
        return self.config.segments_dir

    @property
    def logs_dir(self) -> Path:
        """ログディレクトリパス"""
        return self.config.logs_dir
