"""CLIユーティリティ関数"""


def expand_svtav1_params(params_string: str) -> list[str]:
    """
    カンマ区切りのSvtAv1EncAppパラメータを展開

    Args:
        params_string: カンマ区切りのパラメータ文字列
                      例: "preset=4,crf=30,enable-qm=1"

    Returns:
        展開されたパラメータのリスト
        例: ['--preset', '4', '--crf', '30', '--enable-qm', '1']

    Examples:
        >>> expand_svtav1_params("preset=4,crf=30")
        ['--preset', '4', '--crf', '30']

        >>> expand_svtav1_params("crf=30")
        ['--crf', '30']

        >>> expand_svtav1_params("")
        []
    """
    result = []
    for param in params_string.split(','):
        if '=' in param:
            key, value = param.split('=', 1)
            result.extend([f'--{key}', value])
    return result


def expand_ffmpeg_params(params_string: str) -> list[str]:
    r"""
    カンマ区切りのFFmpegパラメータを展開（\,でエスケープ可能）

    Args:
        params_string: カンマ区切りのパラメータ文字列
                      例: "vf=scale=1920:1080,c:v=libx264"
                      エスケープ例: "vf=scale=1920:1080\,fps=30,pix_fmt=yuv420p10le"

    Returns:
        展開されたパラメータのリスト
        例: ['-vf', 'scale=1920:1080', '-c:v', 'libx264']

    Examples:
        >>> expand_ffmpeg_params("vf=scale=1920:1080")
        ['-vf', 'scale=1920:1080']

        >>> expand_ffmpeg_params("vf=scale=1920:1080,c:v=libx264")
        ['-vf', 'scale=1920:1080', '-c:v', 'libx264']

        >>> expand_ffmpeg_params("vf=scale=1920:1080\\,fps=30,pix_fmt=yuv420p10le")
        ['-vf', 'scale=1920:1080,fps=30', '-pix_fmt', 'yuv420p10le']

        >>> expand_ffmpeg_params("")
        []
    """
    result = []
    # エスケープされたカンマを一時的に置換
    temp_placeholder = '\x00'
    escaped_string = params_string.replace('\\,', temp_placeholder)

    for param in escaped_string.split(','):
        # プレースホルダーを元のカンマに戻す
        param = param.replace(temp_placeholder, ',')
        if '=' in param:
            key, value = param.split('=', 1)
            result.extend([f'-{key}', value])
    return result
