from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class EncodingConfig:
    input_file: Path
    workspace_dir: Path
    parallel_jobs: int
    extra_args: List[str] = field(default_factory=list)
    segment_length: int = 60  # 秒

    def get_gop_size(self) -> int:
        """extra_args から GOP サイズを抽出する。見つからない場合はデフォルト値を返す"""
        # -g オプションを探す
        for i, arg in enumerate(self.extra_args):
            if arg == '-g' and i + 1 < len(self.extra_args):
                try:
                    return int(self.extra_args[i + 1])
                except ValueError:
                    pass
        # デフォルト値: 240フレーム
        return 240
