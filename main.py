"""
main.py

Orchestrates the full pipeline end-to-end:

    Ingestion -> Parser -> Standardizer -> Validator -> Database

Run locally against sample-data/ with:
    python main.py

Run against GCS + BigQuery with:
    python main.py --source gcs --backend bigquery \
        --gcs-bucket my-bucket --bq-project my-project
"""

from __future__ import annotations

import argparse
import logging

from src.database import Database
from src.ingestion import Ingestion
from src.parser import Parser
from src.standardizer import Standardizer
from src.validator import Validator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("main")


def run_pipeline(args: argparse.Namespace) -> None:
    # Initialize Core Pipeline Stages
    ingestion = Ingestion(
        source=args.source,
        local_path=args.local_path,
        gcs_bucket=args.gcs_bucket,
        gcs_prefix=args.gcs_prefix,
    )
    parser = Parser()
    standardizer = Standardizer()
    validator = Validator()
    database = Database(
        backend=args.backend,
        sqlite_path=args.sqlite_path,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        bq_table=args.bq_table,
    )

    file_count = 0
    total_inserted = 0
    total_flagged = 0

    # Process files as an efficient streaming pipeline
    for ingested in ingestion.read_all():
        file_count += 1
        if not ingested.is_valid:
            logger.warning("Skipping unreadable or invalid file: %s", ingested.source_file)
            continue

        # Flatten nested entries for the individual file
        raw_rows = parser.parse(ingested)
        if not raw_rows:
            continue

        # Standardize the file records
        file_canonical_records = [standardizer.standardize(row) for row in raw_rows]

        # FR-1.2 Validation & Deduplication within the document's scoped batch
        validated_records = validator.validate_batch(file_canonical_records)
        flagged_count = sum(1 for v in validated_records if not v.is_clean)
        
        # Persistent storage commit
        inserted = database.insert_records(validated_records)
        
        # Keep running audit tallies
        total_inserted += inserted
        total_flagged += flagged_count

    # Write execution metric footprint to the audit log table
    database.log_run(
        source_description=f"{args.source}_batch_run", 
        row_count=total_inserted, 
        flagged_count=total_flagged
    )

    logger.info(
        "Pipeline batch run complete: files_processed=%d total_rows_inserted=%d total_flagged_anomalies=%d",
        file_count, total_inserted, total_flagged
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Medical data standardization pipeline")
    p.add_argument("--source", choices=["local", "gcs"], default="local")
    p.add_argument("--local-path", default="sample-data")
    p.add_argument("--gcs-bucket", default=None)
    p.add_argument("--gcs-prefix", default="")
    p.add_argument("--backend", choices=["sqlite", "bigquery"], default="sqlite")
    p.add_argument("--sqlite-path", default=None)
    p.add_argument("--bq-project", default=None)
    p.add_argument("--bq-dataset", default=None)
    p.add_argument("--bq-table", default=None)
    return p.parse_args()


if __name__ == "__main__":
    run_pipeline(parse_args())