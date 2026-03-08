import logging
import os
import signal
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List

from ..core.config import EncodingConfig
from ..core.ffmpeg import FFmpegService, SegmentInfo
from ..core.logging_config import setup_file_and_console_logger
from ..core.platform_utils import get_available_signals
from ..core.video_probe import VideoProbe
from ..core.workspace import make_workspace_from_path


def _worker_init():
    """Initialize worker process: reset signal handlers to defaults

    SIGTERM is not available on Windows, so only reset signals
    that are available on the current platform.
    """
    available_signals = get_available_signals()
    for sig in available_signals.values():
        signal.signal(sig, signal.SIG_DFL)


def _format_time(seconds: float) -> str:
    """Format seconds as hh:mm:ss"""
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class EncodingOrchestrator:
    def __init__(
        self,
        config: EncodingConfig,
        log_level: int = logging.INFO
    ):
        self.config = config
        self.start_time = datetime.now()
        self.workspace = make_workspace_from_path(config.workspace_dir)
        self.logger = setup_file_and_console_logger(
            "av1_encoder",
            self.workspace.log_file,
            level=log_level
        )
        self.ffmpeg = FFmpegService()
        self.video_probe = VideoProbe()
        self._main_pid = os.getpid()  # Record main process PID

    def run(self) -> None:
        # Set up signal handlers
        # SIGTERM is not available on Windows, so only register signals
        # that are available on the current platform.
        available_signals = get_available_signals()
        for sig in available_signals.values():
            signal.signal(sig, self._signal_handler)

        try:
            self._encode_segments()
            self._generate_concat_file()
            self._print_completion()
        except KeyboardInterrupt:
            self.logger.error("Processing interrupted")
            sys.exit(1)  # Cross-platform error exit code
        except Exception:
            self.logger.exception("Error")
            raise

    def _signal_handler(self, signum: int, frame) -> None:
        """Handle received signal (main process only)"""
        # Do nothing in worker processes
        if os.getpid() != self._main_pid:
            return

        self.logger.warning("Interrupt signal received. Cleaning up...")
        raise KeyboardInterrupt()

    def _print_completion(self) -> None:
        end_time = datetime.now()
        elapsed = end_time - self.start_time

        self.logger.info(f"Done. Elapsed: {elapsed}")

    def _load_completed_segments(self) -> set[int]:
        """Load the set of already-completed segment indices"""
        completed_file = self.workspace.work_dir / "completed.txt"
        if not completed_file.exists():
            return set()
        with open(completed_file, 'r', encoding='utf-8') as f:
            return {int(line.strip()) for line in f if line.strip()}

    def _mark_segment_completed(self, segment_index: int) -> None:
        """Mark a segment as completed"""
        completed_file = self.workspace.work_dir / "completed.txt"
        with open(completed_file, 'a', encoding='utf-8') as f:
            f.write(f"{segment_index}\n")

    def _encode_segments(self) -> None:
        # Build segment list
        segments = self._list_segments()

        # Load already-completed segments
        completed_indices = self._load_completed_segments()
        pending_segments = [seg for seg in segments if seg.index not in completed_indices]
        completed_count = len(completed_indices)

        if completed_count > 0:
            self.logger.info(f"Skipping {completed_count} already-completed segment(s)")

        if not pending_segments:
            self.logger.info("All segments are already completed")
            return

        # Get total frame count
        total_frames = self.video_probe.get_total_frames(self.config.input_file)

        total_count = len(segments)
        pending_count = len(pending_segments)

        # Print encoding summary
        self.logger.info("=== Encoding info ===")
        self.logger.info(f"Input: {self.config.input_file.name}")
        self.logger.info(f"Total frames: {total_frames}")
        self.logger.info(f"Segments: {total_count} (pending: {pending_count})")
        self.logger.info(f"Parallel jobs: {self.config.parallel_jobs}")

        # Frames per segment
        frames_per_segment = total_frames / total_count

        count = completed_count  # 完了済みセグメント数から開始
        encode_start_time = time.time()

        executor = ProcessPoolExecutor(
            max_workers=self.config.parallel_jobs,
            initializer=_worker_init
        )

        try:
            # Submit only pending segments
            futures = {
                executor.submit(
                    self.ffmpeg.encode_segment,
                    seg,
                    self.config.input_file,
                    self.config
                ): seg.index for seg in pending_segments
            }

            # Process as each segment completes
            for future in as_completed(futures):
                success = future.result()
                segment_idx = futures[future]

                count += 1

                if success:
                    # Mark as completed
                    self._mark_segment_completed(segment_idx)

                    # Calculate progress
                    elapsed = time.time() - encode_start_time
                    completed_in_session = count - completed_count
                    speed_fps = (completed_in_session * frames_per_segment) / elapsed if elapsed > 0 else 0
                    remaining_segments = total_count - count
                    remaining_seconds = (remaining_segments * frames_per_segment) / speed_fps if speed_fps > 0 else 0

                    self.logger.info(
                        f"Done: {segment_idx} ({count}/{total_count}) "
                        f"speed: {speed_fps:.1f}fps remaining: {_format_time(remaining_seconds)}"
                    )
                else:
                    raise RuntimeError(f"Encoding failed for segment {segment_idx}")

            self.logger.info("Encoding complete")
        except KeyboardInterrupt:
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            executor.shutdown(wait=True)

    def _generate_concat_file(self) -> None:
        self.logger.info("Generating concat.txt")

        # List segment files
        segment_files = sorted(self.workspace.work_dir.glob("segment_*.ivf"))

        # Write concat.txt
        concat_txt_content = [f"file '{segment_file.resolve()}'\n" for segment_file in segment_files]
        with open(self.workspace.concat_file, 'w', encoding='utf-8') as f:
            f.writelines(concat_txt_content)

        self.logger.info(f"concat.txt generated: {self.workspace.concat_file}")

    def _list_segments(self) -> List[SegmentInfo]:
        num_segments = self._calc_num_segments()

        # Get frame rate and GOP size
        fps = self.ffmpeg.get_fps(self.config.input_file)
        gop_size = self.config.gop_size

        # Compute GOP-aligned segment duration
        gop_duration = gop_size / fps
        num_gops = round(self.config.segment_length / gop_duration)
        segment_duration = num_gops * gop_duration

        segments: List[SegmentInfo] = []
        for i in range(num_segments):
            start_time = i * segment_duration
            is_final = (i == num_segments - 1)
            segments.append(SegmentInfo(
                index=i,
                start_time=start_time,
                duration=segment_duration,
                is_final=is_final,
                file=self.workspace.work_dir / f"segment_{i:04d}.ivf",
                log_file=self.workspace.work_dir / f"segment_{i:04d}.log"
            ))
        return segments

    def _calc_num_segments(self) -> int:
        duration = self.ffmpeg.get_duration(self.config.input_file)
        return int((duration + self.config.segment_length - 1) // self.config.segment_length)

