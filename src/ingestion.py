"""
ingestion.py

Schema-agnostic ingestion module.

Responsibilities:
- Read JSON from Local Folder or Google Cloud Storage
- Validate JSON
- Generate metadata
- Compute SHA256 checksum
- Skip duplicate files
- Return IngestedRecord objects

No assumptions are made about the JSON schema.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

logger = logging.getLogger(__name__)


@dataclass
class IngestedRecord:
    source_file: str
    raw_json: dict
    checksum: str

    document_id: Optional[str] = None
    correlation_id: Optional[str] = None

    ingested_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    source: str = "local"
    is_valid: bool = True
    is_duplicate: bool = False
    error_message: Optional[str] = None


class DuplicateStore:
    """
    Stores processed file checksums.

    Development:
        SQLite

    Production:
        Replace with BigQuery / Firestore.
    """

    def __init__(self, db_path="pipeline.db"):

        self.conn = sqlite3.connect(db_path)

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_files(

                checksum TEXT PRIMARY KEY,

                processed_at TEXT

            )
            """
        )

        self.conn.commit()

    def exists(self, checksum):

        cursor = self.conn.execute(
            "SELECT 1 FROM processed_files WHERE checksum=?",
            (checksum,),
        )

        return cursor.fetchone() is not None

    def add(self, checksum):

        self.conn.execute(
            "INSERT OR IGNORE INTO processed_files VALUES (?,?)",
            (
                checksum,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

        self.conn.commit()


class Ingestion:

    def __init__(
        self,
        source="local",
        local_path="sample-data",
        gcs_bucket=None,
        gcs_prefix="",
    ):

        self.source = source

        self.local_path = Path(local_path)

        self.gcs_bucket = gcs_bucket

        self.gcs_prefix = gcs_prefix

        self.duplicates = DuplicateStore()

    # ---------------------------

    def read_all(self):

        if self.source == "local":

            yield from self._read_local()

        else:

            yield from self._read_gcs()

    # ---------------------------

    def _read_local(self):

        if not self.local_path.exists():

            raise FileNotFoundError(self.local_path)

        for file in sorted(self.local_path.glob("*.json")):

            yield self._process_bytes(
                file.read_bytes(),
                str(file),
                "local",
            )

    # ---------------------------

    def _read_gcs(self):

        from google.cloud import storage

        client = storage.Client()

        bucket = client.bucket(self.gcs_bucket)

        blobs = bucket.list_blobs(prefix=self.gcs_prefix)

        for blob in blobs:

            if not blob.name.endswith(".json"):

                continue

            yield self._process_bytes(
                blob.download_as_bytes(),
                f"gs://{self.gcs_bucket}/{blob.name}",
                "gcs",
            )

    # ---------------------------

    def _process_bytes(
        self,
        raw_bytes,
        source_file,
        source,
    ):

        try:

            raw_json = json.loads(raw_bytes)

        except json.JSONDecodeError as e:

            return IngestedRecord(
                source_file=source_file,
                raw_json={},
                checksum="",
                source=source,
                is_valid=False,
                error_message=str(e),
            )

        checksum = hashlib.sha256(
            json.dumps(
                raw_json,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()

        duplicate = self.duplicates.exists(checksum)

        if not duplicate:

            self.duplicates.add(checksum)

        return IngestedRecord(

            source_file=source_file,

            raw_json=raw_json,

            checksum=checksum,

            source=source,

            is_duplicate=duplicate,

        )