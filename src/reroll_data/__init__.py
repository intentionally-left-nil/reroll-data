"""Scrape every .whl filename on PyPI into a local SQLite database."""

from __future__ import annotations

from .cli import main

__all__ = ["main"]
