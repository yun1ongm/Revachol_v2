import functools
import os
from typing import Optional


@functools.cache
def is_interactive() -> bool:
    try:
        from IPython import get_ipython  # type: ignore
    except ImportError:
        return False
    ip = get_ipython()
    return ip is not None  # Probably standard Python interpreter


def app() -> Optional[str]:
    return os.environ.get("APP")


def host() -> Optional[str]:
    return os.environ.get("HOST")


def category() -> Optional[str]:
    return os.environ.get("CATEGORY")


def deploy_env() -> str:
    return os.environ.get("DEPLOY_ENV", "canary")


def configs_repo_token() -> str:
    return os.environ["CONTEK_CONFIGS_REPO_TOKEN"]
