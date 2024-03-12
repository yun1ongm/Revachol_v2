import os
import signal
from getpass import getpass
from subprocess import DEVNULL, run
from typing import List

import psutil


def sudo(command: List[str]):
    has_sudo = run(["sudo", "-n", "true"], stderr=DEVNULL).returncode == 0
    cmd = ["sudo", "-S"] + command
    cmd_ni = ["sudo", "-n"] + command
    print(" ".join(cmd))
    if not has_sudo and run(cmd_ni, stderr=DEVNULL, stdout=DEVNULL).returncode != 0:
        password = getpass("Sudo Password: ").encode()
        run(cmd, input=password, stderr=DEVNULL)
    else:
        run(cmd_ni)


def pkill(name: str, sig: int = signal.SIGKILL) -> int:
    cnt = 0
    for p in psutil.process_iter():
        try:
            cmdline = p.cmdline()
            if len(cmdline) > 0:
                exe_path = cmdline[0]
                if os.path.basename(exe_path) == name:
                    p.send_signal(sig)
                    cnt += 1
        except (psutil.AccessDenied, psutil.ZombieProcess):
            continue
        except psutil.NoSuchProcess:
            continue
    return cnt
