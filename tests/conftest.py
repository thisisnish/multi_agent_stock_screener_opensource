"""
tests/conftest.py — Adds the project root to sys.path so that `screener`
is importable without a package install.
"""
import sys
from pathlib import Path

# Project root (one level above tests/)
sys.path.insert(0, str(Path(__file__).parent.parent))
