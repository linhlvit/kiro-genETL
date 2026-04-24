"""Version Store for dbt Job Generator.

In-memory store for mapping version history, supporting
save, retrieve, and list operations.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone

from dbt_job_generator.models.change import MappingVersion, VersionInfo
from dbt_job_generator.models.mapping import MappingSpec


class VersionStore:
    """In-memory version store for mapping specifications."""

    def __init__(self) -> None:
        self._versions: dict[str, list[MappingVersion]] = defaultdict(list)

    def save_version(
        self,
        mapping_name: str,
        mapping: MappingSpec,
        description: str = "",
    ) -> str:
        """Save a new version of a mapping and return the version_id (UUID)."""
        version_id = str(uuid.uuid4())
        version = MappingVersion(
            version_id=version_id,
            mapping_name=mapping_name,
            timestamp=datetime.now(timezone.utc),
            mapping_spec=mapping,
            change_description=description,
        )
        self._versions[mapping_name].append(version)
        return version_id

    def get_version(self, mapping_name: str, version_id: str) -> MappingSpec:
        """Retrieve a specific version of a mapping.

        Raises KeyError if the mapping_name or version_id is not found.
        """
        for version in self._versions.get(mapping_name, []):
            if version.version_id == version_id:
                return version.mapping_spec
        raise KeyError(
            f"Version '{version_id}' not found for mapping '{mapping_name}'"
        )

    def list_versions(self, mapping_name: str) -> list[VersionInfo]:
        """List all versions for a mapping, ordered by save time."""
        return [
            VersionInfo(
                version_id=v.version_id,
                mapping_name=v.mapping_name,
                timestamp=v.timestamp,
                change_description=v.change_description,
            )
            for v in self._versions.get(mapping_name, [])
        ]
