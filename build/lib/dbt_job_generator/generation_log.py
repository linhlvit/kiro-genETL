"""Generation Log — tracks mapping checksums to detect changes.

Stores MD5 hashes of mapping CSV files. On subsequent batch runs,
compares current hashes with stored hashes to determine which
mappings are new, changed, or unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class LogEntry:
    """A single entry in the generation log."""
    mapping_hash: str
    generated_at: str
    sql_file: str
    test_file: str = ""


@dataclass
class GenerationLog:
    """Tracks which mappings have been generated and their checksums."""
    entries: dict[str, LogEntry] = field(default_factory=dict)

    def should_generate(self, mapping_name: str, current_hash: str) -> str:
        """Determine if a mapping should be generated.

        Returns: "new", "changed", or "unchanged".
        """
        if mapping_name not in self.entries:
            return "new"
        if self.entries[mapping_name].mapping_hash != current_hash:
            return "changed"
        return "unchanged"

    def update(self, mapping_name: str, mapping_hash: str,
               sql_file: str, test_file: str = "") -> None:
        """Update the log entry for a mapping."""
        self.entries[mapping_name] = LogEntry(
            mapping_hash=mapping_hash,
            generated_at=datetime.now(timezone.utc).isoformat(),
            sql_file=sql_file,
            test_file=test_file,
        )

def compute_file_hash(file_path: str) -> str:
    """Compute MD5 hash of a file's contents."""
    with open(file_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def load_generation_log(output_dir: str) -> GenerationLog:
    """Load generation log from output directory. Returns empty log if not found."""
    log_path = os.path.join(output_dir, ".generation_log.json")
    if not os.path.isfile(log_path):
        return GenerationLog()
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        entries = {}
        for name, entry_data in data.items():
            entries[name] = LogEntry(
                mapping_hash=entry_data.get("mapping_hash", ""),
                generated_at=entry_data.get("generated_at", ""),
                sql_file=entry_data.get("sql_file", ""),
                test_file=entry_data.get("test_file", ""),
            )
        return GenerationLog(entries=entries)
    except (json.JSONDecodeError, OSError):
        return GenerationLog()


def save_generation_log(output_dir: str, log: GenerationLog) -> str:
    """Save generation log to output directory. Returns the file path."""
    log_path = os.path.join(output_dir, ".generation_log.json")
    os.makedirs(output_dir, exist_ok=True)
    data = {}
    for name, entry in log.entries.items():
        data[name] = {
            "mapping_hash": entry.mapping_hash,
            "generated_at": entry.generated_at,
            "sql_file": entry.sql_file,
            "test_file": entry.test_file,
        }
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return log_path
