"""
dashboard.py

Streamlit dashboard for the medical data standardization pipeline.

Run with:
    streamlit run src/dashboard.py

Features:
    - Upload new JSON documents and execute them through the pipeline
    - Separate clean table views for Lab Reports and Discharge Summaries (Removes empty columns)
    - View all canonical lab records reshaped into a fixed 5-column wide schema per test (FR-2.2)
    - View individual patient profiles and raw JSON audit logs side-by-side (FR-5.2)
    - Dedicated Operational Quality Queue for flagged records (FR-5.3)
    - Per-clinic data quality metric engine calculating comparative rates (FR-5.4)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.database import Database
from src.ingestion import Ingestion, IngestedRecord
from src.parser import Parser
from src.standardizer import Standardizer, CONFIG_DIR
from src.validator import Validator

st.set_page_config(page_title="Medical Data Pipeline Dashboard", layout="wide")

DB = Database(backend="sqlite")


@st.cache_resource
def get_pipeline_components():
    return Parser(), Standardizer(), Validator()


@st.cache_data
def load_defined_tests() -> list[str]:
    """FR-2.2: Dynamically loads defined test arrays from config mapping."""
    try:
        with open(CONFIG_DIR / "test_name_mapping.json", "r", encoding="utf-8") as f:
            mapping = json.load(f)
        return [k for k in mapping.keys() if not k.startswith("_")]
    except Exception:
        return ["HEMOGLOBIN", "WHITE_BLOOD_CELL_COUNT", "PLATELET_COUNT"]


def records_to_dataframe(rows: list[dict], defined_tests: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processes relational database rows into two separate, clean DataFrames:
    one for Lab Tests (pivoted) and one for Discharge Summaries (flattened medications).
    """
    if not rows:
        return pd.DataFrame(), pd.DataFrame()
        
    df = pd.DataFrame(rows)
    
    # Safely unpack relational JSON text buffers
    df["canonical_fields"] = df["canonical_fields_json"].apply(json.loads)
    df["standardization_flags"] = df["standardization_flags"].apply(json.loads)
    df["validation_flags"] = df["validation_flags"].apply(json.loads)
    
    # Safe handling if the 'raw_json' column exists or is missing from legacy tables
    if "raw_json" in df.columns:
        df["raw_json_unpacked"] = df["raw_json"].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.strip() else (x if isinstance(x, dict) else {})
        )
    else:
        df["raw_json_unpacked"] = [{}] * len(df)
    
    # Flatten base canonical attributes
    expanded = pd.json_normalize(df["canonical_fields"])
    base_data = pd.concat([
        df[["id", "record_type", "source_file", "inserted_at", "standardization_flags", "validation_flags", "raw_json_unpacked"]], 
        expanded
    ], axis=1)
    
    base_data["all_flags"] = base_data["standardization_flags"] + base_data["validation_flags"]
    base_data["is_flagged"] = base_data["all_flags"].apply(lambda flags: len(flags) > 0)
    
    # Isolate into separate datasets
    lab_src = base_data[base_data["record_type"] == "lab_test"]
    discharge_src = base_data[base_data["record_type"].isin(["discharge_summary", "discharge_medication"])]
    
    # --- PROCESS LAB REPORT VIEW (PIVOTED FIXED COLUMNS) ---
    lab_final = pd.DataFrame()
    if not lab_src.empty:
        id_vars = ["patient_name", "uhid", "age", "gender", "hospital_name", "report_date", "source_file", "inserted_at", "is_flagged", "all_flags", "raw_json_unpacked"]
        pivot_src = lab_src.copy()
        
        pivot_src["range_str"] = pivot_src.apply(
            lambda r: f"{r.get('range_low') or ''} - {r.get('range_high') or ''}" if r.get("range_low") or r.get("range_high") else None, 
            axis=1
        )
        
        pivoted_list = []
        # FR-2.2: Reshape lab entries into a fixed wide matrix schema grouped by patient identities
        for _, grp in pivot_src.groupby(["patient_name", "report_date"], dropna=False):
            # Snag primary baseline row metadata identifiers safely
            patient_row = {col: grp[col].iloc[0] for col in id_vars if col in grp.columns}
            patient_row["record_type"] = "lab_test"
            patient_row["id"] = grp["id"].iloc[0]
            
            # Seed precisely 5 explicit blank columns for every distinct medical test
            for test in defined_tests:
                patient_row[f"{test}"] = None
                patient_row[f"{test}_Result"] = None
                patient_row[f"{test}_Range"] = None
                patient_row[f"{test}_Unit"] = None
                patient_row[f"{test}_Analytics"] = None
            
            # Inject extracted metrics precisely into matching test parameters
            for _, test_row in grp.iterrows():
                t_name = test_row.get("test_name")
                if t_name in defined_tests:
                    patient_row[f"{t_name}"] = test_row.get("test_name_raw")
                    patient_row[f"{t_name}_Result"] = test_row.get("result_numeric") if test_row.get("is_numeric_result") else test_row.get("result_raw")
                    patient_row[f"{t_name}_Range"] = test_row.get("range_str")
                    patient_row[f"{t_name}_Unit"] = test_row.get("unit")
                    patient_row[f"{t_name}_Analytics"] = test_row.get("test_analytics")
                    
            pivoted_list.append(patient_row)
        lab_final = pd.DataFrame(pivoted_list)

    # --- PROCESS DISCHARGE VIEW (REMOVE ALL LAB COLUMNS) ---
    discharge_final = pd.DataFrame()
    if not discharge_src.empty:
        # Drop columns belonging to the lab layout dynamically to keep it clean
        lab_columns_to_drop = ["test_name", "test_name_raw", "result_raw", "result_numeric", "is_numeric_result", "range_low", "range_high", "unit", "test_analytics"]
        discharge_final = discharge_src.drop(columns=lab_columns_to_drop, errors="ignore").dropna(axis=1, how="all")
        
    return lab_final, discharge_final


def main():
    st.title("🏥 Medical Data Pipeline Dashboard")
    st.caption("Ingestion System Platform Control Tower")

    tab_upload, tab_records, tab_inspector, tab_flagged, tab_analytics = st.tabs(
        ["Upload & Process", "All Records", "🔍 Record Inspector", "⚠️ Flagged Queue", "📊 Operational Analytics"]
    )

    parser, standardizer, validator = get_pipeline_components()
    defined_tests = load_defined_tests()

    # Data layer refresh link
    all_rows = DB.fetch_all_sqlite()
    lab_df, discharge_df = records_to_dataframe(all_rows, defined_tests)

    # Combine dataframes purely for shared utility counters inside Inspector/Flags tabs
    df = pd.concat([lab_df, discharge_df], axis=0, ignore_index=True) if (not lab_df.empty or not discharge_df.empty) else pd.DataFrame()

    with tab_upload:
        st.subheader("Upload JSON documents")
        uploaded_files = st.file_uploader("Choose one or more JSON files", type="json", accept_multiple_files=True)
        if uploaded_files and st.button("Run pipeline on uploaded files"):
            canonical_records = []
            files_failed = 0
            files_processed = 0
            
            for uploaded in uploaded_files:
                try:
                    raw_bytes = uploaded.read()
                    raw_json = json.loads(raw_bytes)
                    
                    ingested = IngestedRecord(
                        source_file=uploaded.name,
                        raw_json=raw_json,
                        checksum="",
                        document_id=raw_json.get("data", {}).get("documentId"),
                        correlation_id=raw_json.get("data", {}).get("correlationId"),
                    )
                    
                    file_rows = parser.parse(ingested)
                    if not file_rows:
                        files_failed += 1
                        continue
                        
                    for row in file_rows:
                        canonical_records.append(standardizer.standardize(row))
                    files_processed += 1
                except Exception as e:
                    files_failed += 1
                    st.error(f"Failed handling raw payload {uploaded.name}: {str(e)}")

            if canonical_records:
                validated = validator.validate_batch(canonical_records)
                inserted = DB.insert_records(validated)
                flagged = sum(1 for v in validated if not v.is_clean)
                
                # FR-5.1: High-fidelity logging audit metrics injection
                DB.log_run(
                    source_description=f"streamlit_upload_batch_{files_processed}_files", 
                    row_count=inserted, 
                    flagged_count=flagged
                )
                st.success(f"Execution run summary: Received={len(uploaded_files)} | Processed={files_processed} | Failed={files_failed} | Rows Inserted={inserted} | Flagged Warnings={flagged}")
                st.rerun()

    with tab_records:
        st.subheader("📋 Canonical Records Registry")
        if lab_df.empty and discharge_df.empty:
            st.info("No structured database records found.")
        else:
            view_mode = st.radio(
                "Select Document View Type",
                ["🧪 Laboratory Diagnostic Reports", "🏥 Hospital Discharge Summaries"],
                horizontal=True
            )
            
            st.markdown("---")
            
            if view_mode == "🧪 Laboratory Diagnostic Reports":
                if lab_df.empty:
                    st.info("No laboratory records currently on file.")
                else:
                    st.markdown("#### Patient Records with Fixed Test Matrix Columns")
                    clean_labs = lab_df.drop(columns=["all_flags", "is_flagged", "raw_json_unpacked", "selector_label"], errors="ignore")
                    st.dataframe(clean_labs, use_container_width=True)
                    
            else:
                if discharge_df.empty:
                    st.info("No hospital discharge summaries currently on file.")
                else:
                    st.markdown("#### Patient Admissions & Prescribed Home Medications")
                    clean_discharge = discharge_df.drop(columns=["all_flags", "is_flagged", "raw_json_unpacked", "selector_label"], errors="ignore")
                    st.dataframe(clean_discharge, use_container_width=True)

    with tab_inspector:
        st.subheader("FR-5.2: Relational Audit Lineage Inspector")
        if df.empty:
            st.info("No medical records available for exploration.")
        else:
            # Dropdown select picker targeting unique data lines
            df["selector_label"] = df.apply(lambda r: f"[{str(r.get('record_type')).upper()}] {r.get('patient_name')} - {r.get('report_date') or r.get('admission_date') or 'Unknown Date'} (File: {r.get('source_file')})", axis=1)
            
            selected_label = st.selectbox("Select a patient record log to inspect details", options=df["selector_label"].unique())
            selected_record = df[df["selector_label"] == selected_label].iloc[0]
            
            col_transformed, col_raw = st.columns(2)
            
            with col_transformed:
                st.markdown("#### ✨ Standardized Transformed Record fields")
                transformed_data_clean = {k: v for k, v in selected_record.to_dict().items() if k not in ["selector_label", "raw_json_unpacked", "all_flags", "has_missing_field", "has_schema_error", "is_duplicate_entry"]}
                st.json(transformed_data_clean)
                
            with col_raw:
                st.markdown("#### 🪵 Original Incoming Raw JSON Payload")
                if isinstance(selected_record.get("raw_json_unpacked"), dict) and selected_record["raw_json_unpacked"]:
                    st.json(selected_record["raw_json_unpacked"])
                else:
                    st.warning("No original source backup JSON logged for this historical row entry.")

    with tab_flagged:
        st.subheader("FR-5.3: Core Operational Quality Validation Queue")
        if df.empty:
            st.info("No entries to track.")
        else:
            flagged_df = df[df["is_flagged"] == True]
            st.metric("Total Flagged Records Pending Review", len(flagged_df))
            
            if not flagged_df.empty:
                display_flagged = flagged_df.drop(columns=["all_flags", "is_flagged", "raw_json_unpacked", "selector_label", "has_missing_field", "has_schema_error", "is_duplicate_entry"], errors="ignore")
                st.dataframe(display_flagged, use_container_width=True)
            else:
                st.success("🎉 Excellent! Zero internal data-quality alerts flagged across currently loaded datasets.")

    with tab_analytics:
        st.subheader("Operational Quality Metrics Control Tower")
        if df.empty:
            st.info("Upload records to view system charts.")
        else:
            # FR-5.4: Calculate Clinic-Level Ratios defensively against float/NaN values
            st.markdown("### 🏢 FR-5.4: Per-Clinic Data Quality Scorecard Summary")
            if "hospital_name" in df.columns and not df["hospital_name"].isna().all():
                
                # Safe checking logic ensuring iterable verification before text containment validation
                df["has_missing_field"] = df["validation_flags"].apply(
                    lambda flags: any("MISSING_FIELD" in str(f) for f in flags) if isinstance(flags, (list, set, tuple)) else False
                )
                df["has_schema_error"] = df["standardization_flags"].apply(
                    lambda flags: any("UNMAPPED" in str(f) for f in flags) if isinstance(flags, (list, set, tuple)) else False
                )
                df["is_duplicate_entry"] = df["validation_flags"].apply(
                    lambda flags: "DUPLICATE_RECORD" in flags if isinstance(flags, (list, set, tuple)) else False
                )

                # Group metrics aggregate parameters by Clinic location Name
                clinic_grp = df.groupby("hospital_name").agg(
                    total_rows=("inserted_at", "count"),
                    missing_fields=("has_missing_field", "sum"),
                    schema_errors=("has_schema_error", "sum"),
                    duplicates=("is_duplicate_entry", "sum")
                ).reset_index()

                # Derive percentage fractions
                clinic_grp["Duplicate Rate (%)"] = ((clinic_grp["duplicates"] / clinic_grp["total_rows"]) * 100).round(2)
                clinic_grp["Error Rate (%)"] = ((clinic_grp["schema_errors"] / clinic_grp["total_rows"]) * 100).round(2)
                clinic_grp["Missing-Field Rate (%)"] = ((clinic_grp["missing_fields"] / clinic_grp["total_rows"]) * 100).round(2)

                st.dataframe(
                    clinic_grp[["hospital_name", "total_rows", "Duplicate Rate (%)", "Error Rate (%)", "Missing-Field Rate (%)"]],
                    use_container_width=True, hide_index=True
                )
            else:
                st.warning("Per-clinic analytics are unavailable—the extracted records do not contain a populated `hospital_name` identifier.")

            st.markdown("---")
            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.write("**Data Intake Volumes categorized by record structure**")
                st.bar_chart(df["record_type"].value_counts())
            with col_chart2:
                all_flags = [flag for flags in df["all_flags"] for flag in flags] if "all_flags" in df.columns else []
                if all_flags:
                    st.write("**Pipeline alert code frequency distribution tracking**")
                    st.bar_chart(pd.Series(all_flags).value_counts())
                else:
                    st.write("No technical anomalies logged.")


if __name__ == "__main__":
    main()