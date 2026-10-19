"""``python -m rs_pipeline`` works without a database for --help."""

from __future__ import annotations

import subprocess
import sys


def test_module_entry_point_prints_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "rs_pipeline", "--help"], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0
    assert "run-all" in result.stdout
