#!/usr/bin/env python3
"""Stop only our Cargo process group before the ephemeral runner fills."""
import os
import shutil
import signal
import subprocess
import sys
import time

MIN_FREE = 2 * 1024**3
root = os.environ["RUNNER_TEMP"]
print(f"disk before command: {shutil.disk_usage(root).free} bytes free", flush=True)
if shutil.disk_usage(root).free < MIN_FREE:
    raise SystemExit("diagnostic disk floor reached before Cargo")
child = subprocess.Popen(sys.argv[1:], start_new_session=True)
while child.poll() is None:
    time.sleep(5)
    free = shutil.disk_usage(root).free
    if free < MIN_FREE:
        print(f"diagnostic disk floor reached: {free} bytes free", flush=True)
        os.killpg(child.pid, signal.SIGTERM)
        try:
            child.wait(timeout=20)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait()
        raise SystemExit(71)
print(f"disk after command: {shutil.disk_usage(root).free} bytes free", flush=True)
raise SystemExit(child.returncode)
