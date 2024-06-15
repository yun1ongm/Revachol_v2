import argparse
import os
from pathlib import Path
from typing import Dict, Optional

import yaml
from expression import Option
from expression.core.option import of_optional

_config = None


def load_yaml(path):
    with open(path, "r") as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def load_yaml_with_import(path: Optional[Path | str]) -> dict:
    if path is None:
        return {}
    else:
        result = load_yaml(path)
        if result is None:
            return {}
        if "import" in result:
            for import_file_name in result["import"]:
                import_path = Path(Path(path).parent, import_file_name)
                import_yaml = load_yaml_with_import(import_path)
                result = import_yaml | result
            del result["import"]
        return result


def get_config(with_import=True, reload=False, parser=None):
    global _config
    if _config is None or reload:
        cfg = {}
        config_path = get_config_path()
        if config_path is not None:
            if with_import:
                cfg = load_yaml_with_import(config_path)
            else:
                cfg = load_yaml(config_path)
        _config = cfg if parser is None else parser(cfg, config_path)
    return _config


def get_config_from_file(with_import=True) -> Option[Dict]:
    config_path = of_optional(get_config_path())
    return config_path.map(lambda p: load_yaml_with_import(p) if with_import else load_yaml(p))


def get_config_path() -> Optional[str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="?", default=None, type=str, help="config file path")
    args, _ = parser.parse_known_args()
    return os.path.expanduser(args.config) if args.config is not None else None


def set_config(config):
    global _config
    _config = config
