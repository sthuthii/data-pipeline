"""
standardizer.py

Normalizes raw extracted fields into the canonical schema using the
config/*.json mapping files:
    - test_name_mapping.json
    - medicine_mapping.json
    - unit_mapping.json

Matching strategy for names:
    1. Exact match (case-insensitive) against a known variant.
    2. Fuzzy match (difflib) against known variants, for OCR-truncated or
       slightly misspelled names (e.g. "aemoglobin" -> "HEMOGLOBIN").
    3. If nothing clears the fuzzy threshold, keep the raw value and flag
       it as UNMAPPED so it surfaces in the dashboard for manual review.

Also normalizes dates, gender, and numeric results.
"""

from __future__ import annotations

import difflib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from src.parser import RawRow

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
FUZZY_MATCH_THRESHOLD = 0.82  # difflib similarity ratio, 0-1

DATE_FORMATS = [
    "%d-%m-%Y",   # 09-10-2025
    "%d-%b-%Y",   # 07-Oct-2025
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
]

GENDER_MAP = {
    "m": "MALE", "male": "MALE", "f": "FEMALE", "female": "FEMALE",
    "o": "OTHER", "other": "OTHER",
}


@dataclass
class CanonicalRecord:
    """A fully standardized record, ready for validation and BigQuery."""

    record_type: str
    source_file: str
    document_id: Optional[str]
    correlation_id: Optional[str]
    canonical_fields: dict = field(default_factory=dict)
    standardization_flags: list[str] = field(default_factory=list)


def _load_config(filename: str) -> dict:
    path = CONFIG_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class NameLookup:
    """Reverse lookup table (raw variant -> canonical name) with fuzzy fallback."""

    def __init__(self, mapping: dict, variants_key: Optional[str] = None):
        self.canonical_names = [k for k in mapping.keys() if not k.startswith("_")]
        self._exact: dict[str, str] = {}
        for canonical, value in mapping.items():
            if canonical.startswith("_"):
                continue
            variants = value.get(variants_key) if variants_key else value
            for variant in variants:
                self._exact[variant.strip().lower()] = canonical

    def resolve(self, raw_value: Optional[str]) -> tuple[Optional[str], bool]:
        """Returns (canonical_name_or_original, was_mapped)."""
        if not raw_value or not str(raw_value).strip():
            return raw_value, False

        cleaned = raw_value.strip().lower()
        if cleaned in self._exact:
            return self._exact[cleaned], True

        # Fuzzy fallback across known variants
        best = difflib.get_close_matches(cleaned, self._exact.keys(), n=1, cutoff=FUZZY_MATCH_THRESHOLD)
        if best:
            return self._exact[best[0]], True

        return raw_value, False


class Standardizer:
    def __init__(self):
        self.test_lookup = NameLookup(_load_config("test_name_mapping.json"))
        self.medicine_lookup = NameLookup(_load_config("medicine_mapping.json"), variants_key="variants")
        self.unit_lookup = NameLookup(_load_config("unit_mapping.json"))
        self._medicine_categories = {
            canonical: value.get("category")
            for canonical, value in _load_config("medicine_mapping.json").items()
            if not canonical.startswith("_")
        }

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def standardize(self, row: RawRow) -> CanonicalRecord:
        if row.record_type == "lab_test":
            return self._standardize_lab_test(row)
        elif row.record_type == "discharge_medication":
            return self._standardize_discharge_medication(row)
        elif row.record_type == "discharge_summary":
            return self._standardize_discharge_summary(row)
        raise ValueError(f"Unknown record_type: {row.record_type}")

    # ------------------------------------------------------------------ #
    # lab_test
    # ------------------------------------------------------------------ #

    def _standardize_lab_test(self, row: RawRow) -> CanonicalRecord:
        f = row.fields
        flags: list[str] = []

        test_name, mapped = self.test_lookup.resolve(f.get("test_name_raw"))
        if not mapped:
            flags.append("UNMAPPED_TEST_NAME")

        unit, unit_mapped = self.unit_lookup.resolve(f.get("unit_raw"))
        if f.get("unit_raw") and not unit_mapped:
            flags.append("UNMAPPED_UNIT")

        numeric_result, is_numeric = self._to_numeric(f.get("result_raw"))
        low, high = self._parse_range(f.get("range_raw"))

        canonical = {
            "patient_name": f.get("patient_name"),
            "uhid": f.get("uhid"),
            "age": self._clean_age(f.get("age")),
            "gender": self._normalize_gender(f.get("gender")),
            "hospital_name": f.get("lab_or_hospital_name"),
            "report_date": self._normalize_date(f.get("report_date")),
            "test_name": test_name,
            "test_name_raw": f.get("test_name_raw"),
            "result_raw": f.get("result_raw"),
            "result_numeric": numeric_result,
            "is_numeric_result": is_numeric,
            "range_low": low,
            "range_high": high,
            "unit": unit,
            "test_analytics": f.get("test_analytics"),
        }

        return CanonicalRecord(
            record_type="lab_test",
            source_file=row.source_file,
            document_id=row.document_id,
            correlation_id=row.correlation_id,
            canonical_fields=canonical,
            standardization_flags=flags,
        )

    # ------------------------------------------------------------------ #
    # discharge_medication
    # ------------------------------------------------------------------ #

    def _standardize_discharge_medication(self, row: RawRow) -> CanonicalRecord:
        f = row.fields
        flags: list[str] = []

        medicine_name, mapped = self.medicine_lookup.resolve(f.get("medicine_name_raw"))
        if f.get("medicine_name_raw") and not mapped:
            flags.append("UNMAPPED_MEDICINE_NAME")

        canonical = {
            "patient_name": f.get("patient_name"),
            "age": self._clean_age(f.get("age")),
            "gender": self._normalize_gender(f.get("gender")),
            "hospital_name": f.get("hospital_name"),
            "admission_date": self._normalize_date(f.get("admission_date_raw")),
            "discharge_date": self._normalize_date(f.get("discharge_date_raw")),
            "diagnosis": f.get("diagnosis"),
            "medicine_name": medicine_name,
            "medicine_name_raw": f.get("medicine_name_raw"),
            "medicine_category": self._medicine_categories.get(medicine_name),
            "dose": f.get("dose_raw"),
            "frequency": f.get("frequency_raw"),
        }

        return CanonicalRecord(
            record_type="discharge_medication",
            source_file=row.source_file,
            document_id=row.document_id,
            correlation_id=row.correlation_id,
            canonical_fields=canonical,
            standardization_flags=flags,
        )

    # ------------------------------------------------------------------ #
    # discharge_summary (no medications present)
    # ------------------------------------------------------------------ #

    def _standardize_discharge_summary(self, row: RawRow) -> CanonicalRecord:
        f = row.fields
        canonical = {
            "patient_name": f.get("patient_name"),
            "age": self._clean_age(f.get("age")),
            "gender": self._normalize_gender(f.get("gender")),
            "hospital_name": f.get("hospital_name"),
            "admission_date": self._normalize_date(f.get("admission_date_raw")),
            "discharge_date": self._normalize_date(f.get("discharge_date_raw")),
            "diagnosis": f.get("diagnosis"),
            "brief_history": f.get("brief_history"),
            "recommendations": f.get("recommendations"),
        }
        return CanonicalRecord(
            record_type="discharge_summary",
            source_file=row.source_file,
            document_id=row.document_id,
            correlation_id=row.correlation_id,
            canonical_fields=canonical,
            standardization_flags=[],
        )

    # ------------------------------------------------------------------ #
    # Field-level helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalize_date(self, date_str: str) -> str:
        if not date_str or not isinstance(date_str, str):
            return None
            
        # Clean up trailing whitespaces
        date_str = date_str.strip()
        
        # Define standard clinical date formats to attempt to sweep
        formats_to_try = [
            "%Y-%m-%d",      # 2025-10-10
            "%d/%m/%Y",      # 10/10/2025
            "%m/%d/%Y",      # 10/10/2025 (US variant)
            "%d/%b/%Y",      # 10/Oct/2025  <-- ADD THIS ONE FOR THE FIX!
            "%d-%b-%Y"       # 10-Oct-2025  (Bonus safety fallback)
        ]
        
        for fmt in formats_to_try:
            try:
                parsed_dt = datetime.strptime(date_str, fmt)
                return parsed_dt.date().isoformat()  # Returns clean "2025-10-10"
            except ValueError:
                continue
                
        # If all options exhaust, log the warning and fallback gracefully
        self.logger.warning(f"Could not parse date: {date_str}")
        return None

    @staticmethod
    def _normalize_gender(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        key = re.sub(r"\[.*?redacted.*?\]", "", raw, flags=re.IGNORECASE).strip().lower()
        return GENDER_MAP.get(key, "UNKNOWN" if key else None)

    @staticmethod
    def _clean_age(raw: Optional[str]) -> Optional[str]:
        if not raw:
            return None
        # Preserve redaction markers as-is; real deployments would decode
        # from the un-redacted source before this stage.
        return raw

    @staticmethod
    def _to_numeric(raw: Optional[str]) -> tuple[Optional[float], bool]:
        if raw is None:
            return None, False
        
        # Strip whitespace and isolate the first numeric component found
        raw_str = str(raw).strip()
        match = re.search(r"[-+]?\d*\.\d+|\d+", raw_str)
        
        if match:
            try:
                return float(match.group()), True
            except ValueError:
                return None, False
                
        return None, False

    @staticmethod
    def _parse_range(raw: Optional[str]) -> tuple[Optional[float], Optional[float]]:
        if not raw:
            return None, None
        match = re.match(r"^\s*([\d.]+)\s*-\s*([\d.]+)\s*$", raw)
        if match:
            return float(match.group(1)), float(match.group(2))
        return None, None


if __name__ == "__main__":
    import os

    from src.ingestion import Ingestion
    from src.parser import Parser

    logging.basicConfig(level=logging.INFO)
    ing = Ingestion(source="local", local_path=os.path.join(os.path.dirname(__file__), "..", "sample-data"))
    parser = Parser()
    standardizer = Standardizer()
    for rec in ing.read_all():
        for row in parser.parse(rec):
            canon = standardizer.standardize(row)
            print(canon.record_type, canon.canonical_fields, canon.standardization_flags)
