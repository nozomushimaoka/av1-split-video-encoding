"""共通ユーティリティ関数"""


def expand_svtav1_params(params_string: str) -> list[str]:
    """
    コロン区切りのSvtAv1EncAppパラメータを展開

    Args:
        params_string: コロン区切りのパラメータ文字列
                      例: "preset=4:crf=30:enable-qm=1"

    Returns:
        展開されたパラメータのリスト
        例: ['--preset', '4', '--crf', '30', '--enable-qm', '1']

    Examples:
        >>> expand_svtav1_params("preset=4:crf=30")
        ['--preset', '4', '--crf', '30']

        >>> expand_svtav1_params("crf=30")
        ['--crf', '30']

        >>> expand_svtav1_params("")
        []
    """
    result = []
    for param in params_string.split(':'):
        if '=' in param:
            key, value = param.split('=', 1)
            result.extend([f'--{key}', value])
    return result
