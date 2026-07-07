"""
parser.py

Detects the document type of each classifier block inside an ingested
record (lab_report or discharge_summary) and flattens it into raw rows:
one row per lab test, one row per discharge medication. No normalization
happens here - that is standardizer.py's job. This module only extracts
what is already in the document.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from src.ingestion import IngestedRecord
from datetime import datetime

logger = logging.getLogger(__name__)

KNOWN_CLASSIFIERS = {"lab_report", "discharge_summary"}


@dataclass
class RawRow:
    """One flattened, un-normalized row extracted from a source document."""

    record_type: str  # "lab_test" or "discharge_medication" or "discharge_summary"
    source_file: str
    document_id: Optional[str]
    correlation_id: Optional[str]
    fields: dict = field(default_factory=dict)


class Parser:
    """Turns an IngestedRecord into a list of RawRow objects."""

    def parse(self, record: IngestedRecord) -> list[RawRow]:
        if not record.is_valid:
            logger.warning("Skipping invalid record: %s (%s)", record.source_file, record.error_message)
            return []

        details = (
            record.raw_json.get("data", {})
            .get("responseDetails", [])
        )
        if not details:
            logger.warning("No responseDetails found in %s", record.source_file)
            return []

        rows: list[RawRow] = []
        for block in details:
            classifier = block.get("classifier")
            status = block.get("status")
            if status != "success":
                logger.warning(
                    "Skipping %s block with status=%s in %s", classifier, status, record.source_file
                )
                continue

            if classifier == "lab_report":
                rows.extend(self._parse_lab_report(block.get("data", {}), record))
            elif classifier == "discharge_summary":
                rows.extend(self._parse_discharge_summary(block.get("data", {}), record))
            else:
                logger.warning("Unknown classifier '%s' in %s", classifier, record.source_file)

        return rows

    # ------------------------------------------------------------------ #
    # lab_report
    # ------------------------------------------------------------------ #

    def _parse_lab_report(self, data: dict, record: IngestedRecord) -> list[RawRow]:
        basic_info = data.get("basic_info", {})
        rows = []
        for test in data.get("report_details", []):
            fields: dict[str, Any] = {
                "patient_name": basic_info.get("patient_name"),
                "uhid": basic_info.get("uhid"),
                "age": basic_info.get("age"),
                "gender": basic_info.get("gender"),
                "lab_or_hospital_name": basic_info.get("lab_or_hospital_name"),
                "report_date": basic_info.get("reports_date"),
                "page_no": test.get("page_no"),
                "test_name_raw": test.get("test_name"),
                "result_raw": test.get("result"),
                "range_raw": test.get("range"),
                "unit_raw": test.get("unit"),
                "test_analytics": test.get("test_analytics"),
            }
            rows.append(
                RawRow(
                    record_type="lab_test",
                    source_file=record.source_file,
                    document_id=record.document_id,
                    correlation_id=record.correlation_id,
                    fields=fields,
                )
            )
        return rows

    # ------------------------------------------------------------------ #
    # discharge_summary
    # ------------------------------------------------------------------ #

    def _parse_discharge_summary(self, data: dict, record: IngestedRecord) -> list[RawRow]:
        common: dict[str, Any] = {
            "patient_name": data.get("patientName"),
            "age": data.get("age"),
            "gender": data.get("gender"),
            "hospital_name": data.get("hospitalName"),
            "hospital_address": data.get("hospitalAddress"),
            "doctor_name": data.get("doctorName"),
            "admission_date_raw": data.get("admissionDate"),
            "discharge_date_raw": data.get("dischargeDate"),
            "diagnosis": data.get("diagnosis"),
            "brief_history": data.get("briefHistory"),
            "general_examinations": data.get("generalExaminations"),
            "recommendations": data.get("recommendations"),
            "ward": data.get("ward"),
        }

        medications = data.get("dischargeMedications", []) or []
        if not medications:
            # No medication list - still emit one summary-level row so the
            # admission is captured in the canonical store.
            fields = dict(common)
            fields.update({"medicine_name_raw": None, "dose_raw": None, "frequency_raw": None})
            return [
                RawRow(
                    record_type="discharge_summary",
                    source_file=record.source_file,
                    document_id=record.document_id,
                    correlation_id=record.correlation_id,
                    fields=fields,
                )
            ]

        rows = []
        for med in medications:
            fields = dict(common)
            fields.update(
                {
                    "medicine_name_raw": med.get("medicine"),
                    "dose_raw": med.get("dose"),
                    "frequency_raw": med.get("frequency"),
                    "medicine_type_raw": med.get("type"),
                }
            )
            rows.append(
                RawRow(
                    record_type="discharge_medication",
                    source_file=record.source_file,
                    document_id=record.document_id,
                    correlation_id=record.correlation_id,
                    fields=fields,
                )
            )
        return rows
    
    def normalize_date(self, date_str:str) ->str:
        if not date_str or not isinstance(date_str, str):
            return None

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


if __name__ == "__main__":
    import logging
    import os

    from src.ingestion import Ingestion

    logging.basicConfig(level=logging.INFO)
    ing = Ingestion(source="local", local_path=os.path.join(os.path.dirname(__file__), "..", "sample-data"))
    parser = Parser()
    for rec in ing.read_all():
        for row in parser.parse(rec):
            print(row.record_type, row.fields)
