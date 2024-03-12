import asyncio
import base64
import io
import logging
import tarfile
import tempfile

import aiohttp
import git
from aiohttp_retry import RetryClient
from git.repo import Repo

from contek_pyutils.file import load_dir

GITHUB_API_ENTRY_POINT = "https://api.github.com"


async def release_from_github(token: str, repo: str, owner: str = "contek-io", release_name="latest") -> dict:
    auth_header = {"authorization": f"token {token}"}
    release_name = f"tags/{release_name}" if release_name != "latest" else "latest"
    async with RetryClient(
        aiohttp.ClientSession(base_url=GITHUB_API_ENTRY_POINT, headers=auth_header),
        raise_for_status=True,
    ) as s:
        release_path = f"/repos/{owner}/{repo}/releases/{release_name}"
        async with s.get(release_path) as resp:
            zip_url: str = (await resp.json())["tarball_url"]
            zip_url = zip_url.removeprefix(GITHUB_API_ENTRY_POINT)
            async with s.get(zip_url) as zip_file:
                zip_content = await zip_file.read()

        with tempfile.TemporaryDirectory() as tmp_dir:
            content = io.BytesIO(zip_content)
            tarfile.open(mode="r:gz", fileobj=content).extractall(tmp_dir)
            return next(iter(load_dir(tmp_dir).values()))


async def file_from_github(token: str, repo: str, path: str, owner: str = "contek-io", branch="master") -> str:
    async with RetryClient(
        aiohttp.ClientSession(
            base_url="https://api.github.com",
            headers={"authorization": f"token {token}"},
        ),
        raise_for_status=True,
    ) as s:
        http_path = f"/repos/{owner}/{repo}/contents/{path}?ref={branch}"
        async with s.get(http_path, headers={"accept": "application/vnd.github.v3.raw"}) as resp:
            return await resp.text()


async def dir_from_github(
    token: str,
    repo: str,
    owner: str = "contek-io",
    branch="master",
    *,
    ignore_hidden: bool = True,
    use_clone=True,
) -> dict:
    if use_clone:
        success = False
        count = 0
        while not success and count < 2:
            url = f"https://git:{token}@github.com/{owner}/{repo}"
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    logging.info(f"Clone {url} to {tmp_dir}")
                    await asyncio.to_thread(lambda: Repo.clone_from(url=url, to_path=tmp_dir, depth=1, branch=branch))
                    success = True
                    return load_dir(tmp_dir)
            except git.GitError:
                logging.exception(f"Clone {url} failed")
                count += 1
                continue
        raise ValueError("Load dir from github failed")
    else:
        base_url = "https://api.github.com"

        def get_url(e):
            return e["url"][len(base_url) :]

        def is_file(e):
            return e["type"] == "blob"

        def is_folder(e):
            return e["type"] == "tree"

        accept_json_header = {"accept": "application/vnd.github+json"}
        async with RetryClient(
            aiohttp.ClientSession(base_url=base_url, headers={"authorization": f"token {token}"})
        ) as s:

            async def get_json_resp(url):
                async with s.get(url, headers=accept_json_header) as resp:
                    return await resp.json()

            async def get_file_content(url):
                async with s.get(url) as resp:
                    json = await resp.json()
                    logging.debug(json)
                    return base64.b64decode(json["content"])

            async def helper(url) -> dict:
                json_res = await get_json_resp(url)
                logging.debug(json_res)

                if ignore_hidden:
                    tree_files = list(filter(lambda x: x["path"][0] != ".", json_res["tree"]))
                else:
                    tree_files = json_res["tree"]

                def choose_job(e):
                    if is_file(e):
                        return get_file_content(get_url(e))
                    elif is_folder(e):
                        return helper(get_url(e))
                    else:
                        raise ValueError(f"Unknown json from github {e}")

                return dict(
                    zip(
                        (e["path"] for e in tree_files),
                        await asyncio.gather(*(choose_job(e) for e in tree_files)),
                    )
                )

            resp_json = await get_json_resp(f"/repos/{owner}/{repo}/commits?sha={branch}")
            logging.debug(resp_json)
            resp_json = await get_json_resp(f'/repos/{owner}/{repo}/commits/{resp_json[0]["sha"]}')
            logging.debug(resp_json)

            return await helper(f'/repos/{owner}/{repo}/git/trees/{resp_json["sha"]}')
