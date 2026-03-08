# AV1 Split Video Encoding

Splits a video into segments and encodes them in parallel using AV1 (SVT-AV1), then reassembles the result. Supports local files and S3.

## Features

- Parallel segment encoding via FFmpeg + SvtAv1EncApp pipeline
- Batch processing from S3 or local directories
- Automatic detection of unprocessed files
- Audio track handling (copy or re-encode)
- Windows and Linux/macOS support

## Requirements

- Python 3.10+
- [FFmpeg](https://ffmpeg.org/download.html) (on PATH)
- [SvtAv1EncApp](https://gitlab.com/AOMediaCodec/SVT-AV1/-/releases) (on PATH)
- AWS credentials (only for S3 features)

## Installation

```bash
pip install -r requirements.txt
```

### Verifying dependencies

```bash
ffmpeg -version
SvtAv1EncApp --help
```

### AWS credentials (S3 only)

```bash
aws configure
```

## Usage

### Local encoding

Encode a single video file using parallel segment processing:

```bash
python -m av1_encoder.encoding input.mkv workspace/ \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6
```

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `input_file` | | yes | Input video file path |
| `workspace` | | yes | Working directory (must already exist) |
| `--parallel` | `-l` | yes | Number of parallel encoding jobs |
| `--gop` | `-g` | yes | GOP size (keyframe interval) |
| `--svtav1-params` | | yes | SvtAv1EncApp parameters (comma-separated, e.g. `crf=30,preset=6`) |
| `--ffmpeg-params` | | no | FFmpeg parameters (comma-separated, e.g. `vf=scale=1920:1080,r=30`) |
| `--hardware-decode` | | no | Hardware decode type[:device] (e.g. `cuda`, `vaapi:/dev/dri/renderD128`, `qsv`) |
| `--verbose` | `-v` | no | Enable DEBUG logging |

`--svtav1-params crf=30,preset=6` expands to `--crf 30 --preset 6` when passed to SvtAv1EncApp. `--ffmpeg-params` works the same way for FFmpeg.

### Hardware-accelerated decoding

Use `--hardware-decode` to offload video decoding to the GPU. This can significantly speed up the decode stage of the pipeline. Because SvtAv1EncApp is a CPU encoder, decoded frames must be transferred back from GPU to system memory — you **must** supply the appropriate `-vf` filter chain via `--ffmpeg-params` to do this.

The `--hardware-decode` flag sets both `-hwaccel` and `-hwaccel_output_format` to the given value, so frames remain in GPU memory after decoding. Without a `hwdownload` filter, FFmpeg cannot pipe them to SvtAv1EncApp and encoding will fail.

#### CUDA (NVIDIA)

```bash
python -m av1_encoder.encoding input.mkv workspace/ \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6 \
    --hardware-decode cuda \
    --ffmpeg-params 'vf=scale_cuda=format=yuv420p10le\,hwdownload\,format=yuv420p10le'
```

The filter chain `scale_cuda=format=yuv420p10le,hwdownload,format=yuv420p10le`:
1. `scale_cuda=format=yuv420p10le` — converts pixel format on the GPU (use `yuv420p` for 8-bit sources)
2. `hwdownload` — transfers frames from GPU to system memory
3. `format=yuv420p10le` — tags the output so FFmpeg knows the CPU-side format

> **Note:** commas inside a filter chain must be escaped as `\,` so that `--ffmpeg-params` does not split them into separate arguments.

#### VAAPI (Intel/AMD on Linux)

```bash
python -m av1_encoder.encoding input.mkv workspace/ \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6 \
    --hardware-decode vaapi:/dev/dri/renderD128 \
    --ffmpeg-params 'vf=hwdownload\,format=nv12'
```

#### QSV (Intel Quick Sync)

```bash
python -m av1_encoder.encoding input.mkv workspace/ \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6 \
    --hardware-decode qsv \
    --ffmpeg-params 'vf=hwdownload\,format=nv12'
```

| Argument | Format | Description |
|----------|--------|-------------|
| `--hardware-decode` | `TYPE` or `TYPE:DEVICE` | Hardware decoder to use (`cuda`, `vaapi`, `qsv`). Append `:DEVICE` for VAAPI/QSV (e.g. `vaapi:/dev/dri/renderD128`) |

### Find pending files

List input files that don't yet have a corresponding output file. Works with local directories and S3:

```bash
# Local
python -m av1_encoder.list_pending \
    --input-dir /path/to/input \
    --output-dir /path/to/output

# S3
python -m av1_encoder.list_pending \
    --input-dir s3://my-bucket/input/ \
    --output-dir s3://my-bucket/output/

# Save to file for batch encoding
python -m av1_encoder.list_pending \
    --input-dir s3://my-bucket/input/ \
    --output-dir s3://my-bucket/output/ \
    > pending.txt
```

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--input-dir` | `-i` | yes | Input directory (local path or S3 URI) |
| `--output-dir` | `-o` | yes | Output directory (local path or S3 URI) |
| `--verbose` | `-v` | no | Enable DEBUG logging |

### Batch encoding

Encode all files listed in a pending file. Input and output can be local paths or S3 URIs:

```bash
python -m av1_encoder.s3 \
    --pending-files pending.txt \
    --output-dir s3://my-bucket/output/ \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6
```

| Argument | Short | Required | Description |
|----------|-------|----------|-------------|
| `--pending-files` | | yes | Path to file listing inputs (output of `list_pending`) |
| `--output-dir` | `-o` | no | Output directory — local path or S3 URI (default: `.`) |
| `--workspace-base` | `-b` | no | Base directory for per-file workspaces (default: `.`) |
| `--parallel` | `-l` | yes | Number of parallel encoding jobs |
| `--gop` | `-g` | yes | GOP size (keyframe interval) |
| `--svtav1-params` | | yes | SvtAv1EncApp parameters (comma-separated) |
| `--ffmpeg-params` | | no | FFmpeg parameters (comma-separated) |
| `--audio-params` | | no | Audio parameters (comma-separated, e.g. `c:a=aac,b:a=128k`) |
| `--hardware-decode` | | no | Hardware decode type[:device] (e.g. `cuda`, `vaapi:/dev/dri/renderD128`, `qsv`) |
| `--verbose` | `-v` | no | Enable DEBUG logging |

When `--audio-params` is omitted, audio is copied as-is. `c:a=aac,b:a=128k` expands to `-c:a aac -b:a 128k`.

### Typical S3 workflow

```bash
# 1. Find files that need encoding
python -m av1_encoder.list_pending \
    --input-dir s3://my-bucket/input/ \
    --output-dir s3://my-bucket/output/ \
    > pending.txt

# 2. Encode and upload results
python -m av1_encoder.s3 \
    --pending-files pending.txt \
    --output-dir s3://my-bucket/output/ \
    --parallel 8 \
    --gop 240 \
    --svtav1-params crf=30,preset=6
```

## Processing pipeline

1. **Segment split** — video is split into 60-second segments aligned to GOP boundaries
2. **Parallel encode** — each segment is decoded by FFmpeg and encoded by SvtAv1EncApp
3. **Merge** — encoded segments are concatenated with FFmpeg
4. **Audio mux** — audio is extracted from the source and muxed into the final output

Completed segments are tracked in `completed.txt`, so an interrupted encode can be resumed by re-running the same command — already-finished segments are skipped automatically.

## Output layout

```
workspace/
├── output.mkv       # final output
├── main.log         # overall log
├── concat.txt       # segment list for FFmpeg concat
├── completed.txt    # completed segment indices (for resume)
├── segment_0000.log # per-segment encode log
├── segment_0001.log
└── ...
```

Intermediate `.ivf` segment files are deleted after a successful encode.

## Project structure

```
av1-split-video-encoding/
├── av1_encoder/
│   ├── core/                  # shared utilities
│   │   ├── config.py          # EncodingConfig dataclass
│   │   ├── workspace.py       # workspace management
│   │   ├── logging_config.py  # logging setup
│   │   ├── video_probe.py     # ffprobe helpers
│   │   ├── command_builder.py # command construction
│   │   ├── path_utils.py      # local/S3 path helpers
│   │   ├── platform_utils.py  # platform detection
│   │   └── ffmpeg.py          # FFmpeg service
│   ├── encoding/              # local encoding CLI
│   │   ├── cli.py
│   │   └── encoder.py
│   ├── s3/                    # batch encoding CLI
│   │   ├── cli.py
│   │   ├── pipeline.py        # S3 download/upload
│   │   ├── batch_orchestrator.py
│   │   ├── file_processor.py
│   │   └── video_merger.py
│   ├── list_pending/          # pending file detection CLI
│   │   ├── cli.py
│   │   └── pending.py
│   └── cli_utils.py           # shared CLI param expansion
├── scripts/                   # helper scripts
├── tests/                     # test suite
├── Dockerfile                 # containerised dev environment
├── jump_in.sh                 # start dev container (Podman)
└── requirements.txt
```

## Development environment

A Podman-based dev container is provided with Claude Code pre-installed.

```bash
# Start container (builds image on first run)
./jump_in.sh

# Mount SSH keys and agent (for git push, remote access)
./jump_in.sh --ssh

# Mount GPG keys and agent (for commit signing)
./jump_in.sh --gpg

# Mount ~/.claude and ~/.claude.json read-write (for Claude Code config)
./jump_in.sh --claude

# Combine flags as needed
./jump_in.sh --ssh --gpg --claude
```

The script mounts the current directory as `/workspace`. Each flag is opt-in and prints a warning when used, since it grants the container access to your credentials.

## Troubleshooting

**`SvtAv1EncApp` not found** — ensure the binary is on your PATH:
```bash
which SvtAv1EncApp   # Linux/macOS
where SvtAv1EncApp   # Windows
```

**Out of memory** — reduce `--parallel`:
```bash
--parallel 2
```

**Windows: Ctrl+C only** — `SIGTERM` is not available on Windows; use `Ctrl+C` to interrupt encoding.

**Windows: paths with spaces** — quote the workspace path:
```bash
python -m av1_encoder.encoding input.mkv "C:\My Videos\workspace" --parallel 4 --gop 240 --svtav1-params crf=30,preset=6
```

**S3 access error** — check credentials and bucket permissions:
```bash
aws s3 ls s3://my-bucket/
```

