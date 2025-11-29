"""ログ設定モジュール

アプリケーション全体で使用するロガー設定を提供する。
各モジュールで個別にログ設定を行う代わりに、このモジュールの関数を使用する。
"""
import logging
import sys
from pathlib import Path
from typing import Optional


def setup_file_and_console_logger(
    name: str,
    log_file: Path,
    level: int = logging.INFO,
    log_format: str = '[%(asctime)s] %(message)s',
    date_format: str = '%Y-%m-%d %H:%M:%S'
) -> logging.Logger:
    """ファイルとコンソールの両方に出力するロガーを設定する。

    Args:
        name: ロガー名
        log_file: ログファイルのパス
        level: ログレベル（デフォルト: INFO）
        log_format: ログフォーマット
        date_format: 日付フォーマット

    Returns:
        設定済みのロガー
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 既存のハンドラをクリア
    logger.handlers.clear()

    # フォーマッター
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # ファイルハンドラ
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # コンソールハンドラ
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def setup_console_logger(
    name: str,
    level: int = logging.INFO,
    stream: Optional[object] = None,
    log_format: str = '[%(asctime)s] %(levelname)s: %(message)s',
    date_format: str = '%Y-%m-%d %H:%M:%S',
    propagate: bool = False
) -> logging.Logger:
    """コンソール（stderr）のみに出力するロガーを設定する。

    Args:
        name: ロガー名
        level: ログレベル（デフォルト: INFO）
        stream: 出力先ストリーム（デフォルト: sys.stderr）
        log_format: ログフォーマット
        date_format: 日付フォーマット
        propagate: 親ロガーへの伝播（デフォルト: False）

    Returns:
        設定済みのロガー
    """
    logger = logging.getLogger(name)

    # 既にハンドラーが設定されている場合はスキップ
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = propagate

    # フォーマッター
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # コンソールハンドラ
    output_stream = stream if stream is not None else sys.stderr
    console_handler = logging.StreamHandler(output_stream)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def setup_segment_logger(
    segment_idx: int,
    log_file: Path,
    level: int = logging.DEBUG
) -> logging.Logger:
    """セグメントエンコード用のロガーを設定する。

    各セグメントは独立したログファイルに出力される。
    他のロガーへの伝播は無効化される。

    Args:
        segment_idx: セグメントインデックス
        log_file: ログファイルのパス
        level: ログレベル（デフォルト: DEBUG）

    Returns:
        設定済みのセグメントロガー
    """
    logger = logging.getLogger(f"av1_encoder.segment_{segment_idx}")
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    # ファイルハンドラを追加
    file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
    file_handler.setLevel(level)
    formatter = logging.Formatter(
        '[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def cleanup_logger(logger: logging.Logger) -> None:
    """ロガーのハンドラをクリーンアップする。

    Args:
        logger: クリーンアップするロガー
    """
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
