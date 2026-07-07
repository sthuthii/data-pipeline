"""
database.py

Persists validated canonical records. Two backends:

    - "bigquery": inserts into a BigQuery dataset/table (production/GCP).
    - "sqlite":   writes to a local SQLite file (development, no GCP
                  credentials needed). This is the default so the pipeline
                  is runnable end-to-end out of the box.

Also maintains a lightweight processing log table so each pipeline run
is auditable (files processed, row counts, flag counts).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.validator import ValidatedRecord

logger = logging.getLogger(__name__)

DEFAULT_SQLITE_PATH = Path(__file__).resolve().parent.parent / "pipeline_output.db"

CANONICAL_TABLE = "canonical_records"
LOG_TABLE = "processing_log"


class Database:
    def __init__(
        self,
        backend: str = "sqlite",
        sqlite_path: Optional[str] = None,
        bq_project: Optional[str] = None,
        bq_dataset: Optional[str] = None,
        bq_table: Optional[str] = None,
    ):
        if backend not in ("sqlite", "bigquery"):
            raise ValueError("backend must be 'sqlite' or 'bigquery'")
        self.backend = backend
        self.sqlite_path = sqlite_path or str(DEFAULT_SQLITE_PATH)
        self.bq_project = bq_project or os.environ.get("GCP_PROJECT_ID")
        self.bq_dataset = bq_dataset or os.environ.get("BQ_DATASET", "medical_data")
        self.bq_table = bq_table or os.environ.get("BQ_TABLE", CANONICAL_TABLE)

        if backend == "sqlite":
            self._init_sqlite()

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def insert_records(self, records: list[ValidatedRecord]) -> int:
        if self.backend == "sqlite":
            return self._insert_sqlite(records)
        return self._insert_bigquery(records)

    def log_run(self, source_description: str, row_count: int, flagged_count: int) -> None:
        if self.backend == "sqlite":
            self._log_run_sqlite(source_description, row_count, flagged_count)
        else:
            logger.info(
                "[bigquery] run complete: source=%s rows=%d flagged=%d",
                source_description, row_count, flagged_count,
            )

    # ------------------------------------------------------------------ #
    # SQLite backend (default, for local development)
    # ------------------------------------------------------------------ #

    def _init_sqlite(self) -> None:
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {CANONICAL_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_type TEXT,
                    source_file TEXT,
                    document_id TEXT,
                    correlation_id TEXT,
                    canonical_fields_json TEXT,
                    standardization_flags TEXT,
                    validation_flags TEXT,
                    raw_payload_json TEXT,
                    inserted_at TEXT
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_at TEXT,
                    source_description TEXT,
                    row_count INTEGER,
                    flagged_count INTEGER
                )
                """
            )
            conn.commit()

    def _insert_sqlite(self, records: list[ValidatedRecord]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for v in records:
            c = v.canonical
            rows.append(
                (
                    c.record_type,
                    c.source_file,
                    c.document_id,
                    c.correlation_id,
                    json.dumps(c.canonical_fields, default=str),
                    json.dumps(c.standardization_flags),
                    json.dumps(v.validation_flags),
                    getattr(c, "raw_payload_json", "{}"),
                    now,
                )
            )
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.executemany(
                f"""
                INSERT INTO {CANONICAL_TABLE}
                (record_type, source_file, document_id, correlation_id,
                 canonical_fields_json, standardization_flags, validation_flags, raw_payload_json, inserted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
        logger.info("Inserted %d records into sqlite:%s", len(rows), self.sqlite_path)
        return len(rows)

    def _log_run_sqlite(self, source_description: str, row_count: int, flagged_count: int) -> None:
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute(
                f"INSERT INTO {LOG_TABLE} (run_at, source_description, row_count, flagged_count) VALUES (?, ?, ?, ?)",
                (datetime.now(timezone.utc).isoformat(), source_description, row_count, flagged_count),
            )
            conn.commit()

    # ------------------------------------------------------------------ #
    # BigQuery backend (production/GCP)
    # ------------------------------------------------------------------ #

    def _insert_bigquery(self, records: list[ValidatedRecord]) -> int:
        try:
            from google.cloud import bigquery  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "google-cloud-bigquery is required for the bigquery backend. "
                "Install it with: pip install google-cloud-bigquery"
            ) from exc

        client = bigquery.Client(project=self.bq_project)
        table_ref = f"{self.bq_project}.{self.bq_dataset}.{self.bq_table}"

        rows_to_insert = []
        for v in records:
            c = v.canonical
            rows_to_insert.append(
                {
                    "record_type": c.record_type,
                    "source_file": c.source_file,
                    "document_id": c.document_id,
                    "correlation_id": c.correlation_id,
                    "canonical_fields_json": json.dumps(c.canonical_fields, default=str),
                    "standardization_flags": json.dumps(c.standardization_flags),
                    "validation_flags": json.dumps(v.validation_flags),
                    "raw_payload_json": getattr(c, "raw_payload_json", "{}"),
                    "inserted_at": datetime.now(timezone.utc).isoformat(),
                }
            )

        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            logger.error("BigQuery insert errors: %s", errors)
        else:
            logger.info("Inserted %d records into %s", len(rows_to_insert), table_ref)
        return len(rows_to_insert)

    # ------------------------------------------------------------------ #
    # Unified Read-back API
    # ------------------------------------------------------------------ #

    def fetch_all(self) -> list[dict]:
        """Unified data extractor supporting active SQLite or GCP BigQuery engine configurations."""
        if self.backend == "sqlite":
            with sqlite3.connect(self.sqlite_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(f"SELECT * FROM {CANONICAL_TABLE} ORDER BY id DESC")
                return [dict(row) for row in cur.fetchall()]
        
        # BigQuery Production Read-back
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ImportError("google-cloud-bigquery is required for the bigquery backend.") from exc

        client = bigquery.Client(project=self.bq_project)
        table_ref = f"{self.bq_project}.{self.bq_dataset}.{self.bq_table}"
        
        query = f"SELECT * FROM `{table_ref}` ORDER BY inserted_at DESC"
        query_job = client.query(query)
        results = query_job.result()
        
        # Format rows uniformly as standard Python dictionaries containing primitive mappings
        return [dict(row.items()) for row in results]