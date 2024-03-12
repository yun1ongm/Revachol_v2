import logging
import os
import pathlib
import pickle
import shutil
import tempfile
from functools import cache
from pathlib import Path
from typing import Dict, List, Optional, Set

import aiohttp
import pandas as pd
from expression import Nothing, Option, Some

from contek_pyutils.func.core import none_or


def pickle_dump_to_temp(obj, prefix=None, suffix=None, tmp_dir=None) -> Option[str]:
    tmp_dir = none_or(tmp_dir, tempfile.gettempdir())
    _, _, free = shutil.disk_usage(tmp_dir)
    if free < 2**30:
        logging.error(f"No enough space to dump temp obj, {free // 2 ** 30}GiB left")
        return Nothing
    try:
        _, tmp = tempfile.mkstemp(dir=tmp_dir, prefix=prefix, suffix=suffix)
        with open(tmp, "wb") as tmp_f:
            pickle.dump(obj, tmp_f)
        return Some(tmp)
    except Exception:
        logging.exception("Something wrong when dumping temp obj")
        return Nothing


def tree(
    target_files: Dict[str, bool],
    root: str,
    ignore_files: Optional[Set[str]] = None,
    prefix: str = "",
    suffix: str = "",
) -> List[str]:
    ignore_files = ignore_files if ignore_files is not None else set()
    file_list = list()
    for filename in os.listdir(root):
        if not filename.startswith(".") and filename not in ignore_files:
            abs_path = os.path.join(root, filename)
            if os.path.isdir(abs_path):
                file_list.extend(tree(target_files, abs_path, ignore_files, prefix, suffix))
            elif filename.startswith(prefix) and filename.endswith(suffix) and filename in target_files.keys():
                target_files[filename] = True
                file_list.append(abs_path)
    return file_list


def df_to_csv(file: str | Path, df: pd.DataFrame):
    df.to_csv(file, index=False)


def csv_to_df(file: str | Path) -> pd.DataFrame:
    return pd.read_csv(file)


@cache
def get_session(host: str, token: str):
    s = aiohttp.ClientSession(base_url=host, headers={"authorization": f"token {token}"})
    return s


def check_http_status_code(resp: aiohttp.ClientResponse) -> aiohttp.ClientResponse:
    if resp.status != 200:
        raise Exception(resp.content)
    return resp


def remove_dir(top: str):
    for root, dirs, files in os.walk(top, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            os.rmdir(os.path.join(root, name))


def load_dir(base: str | Path):
    base = pathlib.Path(base).expanduser()
    if not base.is_dir():
        raise ValueError(f"{base} is not a directory")
    res = {}
    to_visit = [base]
    while to_visit:
        d = to_visit.pop(-1)
        if d.is_dir():
            to_visit.extend(filter(lambda x: x.stem[0] != ".", d.iterdir()))
        else:
            content = d.read_text()
            rel_dir = d.relative_to(base).parts
            dict_to_write = res
            for p in rel_dir[:-1]:
                dict_to_write = dict_to_write.setdefault(p, {})
            dict_to_write[rel_dir[-1]] = content
    return res


def ftools_cached_ratio(name: str | Path):
    from contek_pyutils.ftools import fincore_ratio  # type: ignore

    with open(name) as f:
        ratio = fincore_ratio(f.fileno())
    return ratio[0] / ratio[1]


def ftools_fadvise(name: str | Path, advice: str):
    from contek_pyutils.ftools import fadvise  # type: ignore

    with open(name) as f:
        fadvise(f.fileno(), mode=advice)
