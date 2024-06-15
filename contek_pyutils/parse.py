def try_parse_float(value, default_value=0.0):
    if value is None:
        return default_value
    try:
        return float(value)
    except ValueError:
        return default_value


def try_parse_int(value, default_value=0):
    if value is None:
        return default_value
    try:
        return int(value)
    except ValueError:
        return default_value


def try_parse_str(value, default_value=""):
    if value is None:
        return default_value
    try:
        return str(value)
    except ValueError:
        return default_value
