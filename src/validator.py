"""
validator.py

Validates canonical records before they are written to BigQuery:
    - Mandatory field presence
    - Numeric result parsing (already attempted in standardizer.py; here
      we just check the outcome)
    - Reference range / outlier checks for lab tests
    - Configurable duplicate detection within a processed batch

Produces a list of validation flags per record. Records are not dropped
on failed validation - they are tagged, so the dashboard can surface them
for review rather than silently losing data.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from src.standardizer import CanonicalRecord

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"

MANDATORY_FIELDS = {
    "lab_test": ["patient_name", "test_name", "report_date"],
    "discharge_medication": ["patient_name", "medicine_name", "admission_date"],
    "discharge_summary": ["patient_name", "admission_date", "diagnosis"],
}


@dataclass
class ValidatedRecord:
    canonical: CanonicalRecord
    validation_flags: list[str] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return len(self.validation_flags) == 0 and len(self.canonical.standardization_flags) == 0


class Validator:
    def __init__(self):
        # Load reference ranges
        with open(CONFIG_DIR / "reference_ranges.json", "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.reference_ranges = {k: v for k, v in raw.items() if not k.startswith("_")}
        
        # FR-1.2: Load configurable deduplication rules
        with open(CONFIG_DIR / "dedup_rules.json", "r", encoding="utf-8") as f:
            self.dedup_rules = json.load(f)
            
        self._seen_keys: set[tuple] = set()

    def validate_batch(self, records: list[CanonicalRecord]) -> list[ValidatedRecord]:
        """Validate a batch, including duplicate detection across the batch."""
        self._seen_keys.clear()
        return [self.validate(record) for record in records]

    def validate(self, record: CanonicalRecord) -> ValidatedRecord:
        flags: list[str] = []
        fields = record.canonical_fields

        flags.extend(self._check_mandatory_fields(record.record_type, fields))

        if record.record_type == "lab_test":
            flags.extend(self._check_lab_test(fields))

        flags.extend(self._check_duplicate(record))

        return ValidatedRecord(canonical=record, validation_flags=flags)

    # ------------------------------------------------------------------ #
    # Individual checks
    # ------------------------------------------------------------------ #

    def _check_mandatory_fields(self, record_type: str, fields: dict) -> list[str]:
        flags = []
        for required in MANDATORY_FIELDS.get(record_type, []):
            if not fields.get(required):
                flags.append(f"MISSING_FIELD:{required}")
        return flags

    def _check_lab_test(self, fields: dict) -> list[str]:
        flags = []
        test_name = fields.get("test_name")
        result = fields.get("result_numeric")
        is_numeric = fields.get("is_numeric_result")

        # Defensive handling for missing raw results to avoid false positives on structural placeholders
        if fields.get("result_raw") is None and not test_name:
            return flags

        # FR-3.4: Flag non-numeric occurrences where numbers are expected
        if not is_numeric:
            flags.append("NON_NUMERIC_RESULT")
            fields["test_analytics"] = "Invalid"
            return flags

        ref = self.reference_ranges.get(test_name)
        if ref is None:
            flags.append("NO_REFERENCE_RANGE")
            fields["test_analytics"] = "Invalid"
            return flags

        if result is not None:
            low = ref["low"]
            high = ref["high"]
            
            # FR-3.2: Establish extreme plausibility thresholds (e.g., 5x the normal bounds)
            outlier_low = low * 0.2
            outlier_high = high * 5.0

            if result <= outlier_low or result >= outlier_high:
                flags.append("OUTLIER")
                fields["test_analytics"] = "Outlier"  # FR-3.3
            elif result < low:
                flags.append("OUT_OF_RANGE")
                fields["test_analytics"] = "Below Range"  # FR-3.3
            elif result > high:
                flags.append("OUT_OF_RANGE")
                fields["test_analytics"] = "Above Range"  # FR-3.3
            else:
                fields["test_analytics"] = "Within Range"  # FR-3.3

        return flags

    def _check_duplicate(self, record: CanonicalRecord) -> list[str]:
        fields = record.canonical_fields
        record_type = record.record_type
        
        # FR-1.2 Fallback to patient_name if configured fallback field isn't populated
        identity_fields = self.dedup_rules.get(record_type, [])
        
        key_list = [record_type]
        for field_name in identity_fields:
            val = fields.get(field_name)
            if field_name == "uhid" and not val:
                val = fields.get("patient_name")
            key_list.append(val)
            
        key = tuple(key_list)

        if key in self._seen_keys:
            return ["DUPLICATE_RECORD"]
        self._seen_keys.add(key)
        return []


if __name__ == "__main__":
    import os

    from src.ingestion import Ingestion
    from src.parser import Parser
    from src.standardizer import Standardizer

    logging.basicConfig(level=logging.INFO)
    ing = Ingestion(source="local", local_path=os.path.join(os.path.dirname(__file__), "..", "sample-data"))
    parser = Parser()
    standardizer = Standardizer()
    validator = Validator()

    all_canonical = []
    for rec in ing.read_all():
        for row in parser.parse(rec):
            all_canonical.append(standardizer.standardize(row))

    for validated in validator.validate_batch(all_canonical):
        print(validated.canonical.record_type, validated.validation_flags, validated.canonical.canonical_fields)