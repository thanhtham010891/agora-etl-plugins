from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

_CHILD_EXIT_CODE = 88


def run_process_death_child(
    config_path: Path, *, timeout_s: float = 30.0
) -> subprocess.CompletedProcess[str]:
    plugin_root = Path(__file__).resolve().parents[2]
    core_src = plugin_root.parent / "agora" / "src"
    plugin_src = plugin_root / "src"
    existing_pythonpath = os.environ.get("PYTHONPATH", "")
    pythonpath = os.pathsep.join(
        str(path) for path in (plugin_src, core_src, existing_pythonpath) if str(path)
    )
    env = {**os.environ, "PYTHONPATH": pythonpath}
    return subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("_process_death_child.py")),
            str(config_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        env=env,
    )


def assert_process_died_after_checkpoint(
    completed: subprocess.CompletedProcess[str],
) -> None:
    assert completed.returncode == _CHILD_EXIT_CODE, (
        "Expected child to exit immediately after checkpoint save.\n"
        f"returncode={completed.returncode}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def read_jsonl(path: Path) -> list[Any]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
