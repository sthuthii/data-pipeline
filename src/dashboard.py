"""
dashboard.py

Enterprise Clinical Data Quality & Operational Analytics Platform.
Provides comprehensive oversight of medical record ingestion, vocabulary alignment,
and physiological range validation across multi-clinic feeds.
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

# Configure professional interface constraints
st.set_page_config(
    page_title="Veritas Claims Analytics - Data Quality Control Tower", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply global CSS override to enforce corporate styling guidelines
st.markdown("""
    <style>
        .reportview-container { background: #f8f9fa; }
        h1 { color: #1e293b; font-weight: 700; font-size: 2.25rem !important; letter-spacing: -0.025em; }
        h2 { color: #334155; font-weight: 600; font-size: 1.5rem !important; margin-top: 1.5rem; }
        h3 { color: #475569; font-weight: 600; font-size: 1.15rem !important; }
        .stTabs [data-baseweb="tab-list"] { gap: 8px; border-bottom: 2px solid #e2e8f0; }
        .stTabs [data-baseweb="tab"] { 
            padding: 12px 20px; 
            background-color: transparent; 
            border-radius: 4px 4px 0 0; 
            font-weight: 500; 
            color: #64748b;
        }
        .stTabs [aria-selected="true"] { 
            background-color: #ffffff !important; 
            border: 1px solid #e2e8f0 !important; 
            border-bottom: 2px solid #2563eb !important; 
            color: #2563eb !important; 
        }
        div[data-testid="stMetricValue"] { font-size: 2rem !important; font-weight: 700; color: #0f172a; }
        div[data-testid="stMetricLabel"] { font-size: 0.875rem !important; font-weight: 500; color: #64748b; }
    </style>
""", unsafe_allow_html=True)

# Initialize data warehouse connection backend
DB = Database()


@st.cache_resource
def get_pipeline_components():
    return Parser(), Standardizer(), Validator()


@st.cache_data
def load_defined_tests() -> list[str]:
    """Dynamically extracts target laboratory markers from configuration directory mapping dictionaries."""
    try:
        with open(CONFIG_DIR / "test_name_mapping.json", "r", encoding="utf-8") as f:
            mapping = json.load(f)
        return [k for k in mapping.keys() if not k.startswith("_")]
    except Exception:
        return ["HEMOGLOBIN", "WHITE_BLOOD_CELL_COUNT", "PLATELET_COUNT"]


def records_to_dataframe(rows: list[dict], defined_tests: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Normalizes un-flattened warehouse records into partitioned downstream data matrices.
    Returns structurally separate matrices for Lab Diagnostics and Discharge Records.
    """
    if not rows:
        return pd.DataFrame(), pd.DataFrame()
        
    df = pd.DataFrame(rows)
    
    # Structural parsing of serialized payload matrices
    df["canonical_fields"] = df["canonical_fields_json"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    df["standardization_flags"] = df["standardization_flags"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    df["validation_flags"] = df["validation_flags"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    
    # Audit trail retention alignment
    if "raw_payload_json" in df.columns:
        df["raw_json_unpacked"] = df["raw_payload_json"].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.strip() else (x if isinstance(x, dict) else {})
        )
    elif "raw_json" in df.columns:
        df["raw_json_unpacked"] = df["raw_json"].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.strip() else (x if isinstance(x, dict) else {})
        )
    else:
        df["raw_json_unpacked"] = [{}] * len(df)
    
    # Context flattening operation loops
    expanded = pd.json_normalize(df["canonical_fields"])
    base_data = pd.concat([
        df[["id", "record_type", "source_file", "inserted_at", "standardization_flags", "validation_flags", "raw_json_unpacked"]], 
        expanded
    ], axis=1)
    
    base_data["all_flags"] = base_data["standardization_flags"] + base_data["validation_flags"]
    base_data["is_flagged"] = base_data["all_flags"].apply(lambda flags: len(flags) > 0)
    
    lab_src = base_data[base_data["record_type"] == "lab_test"]
    discharge_src = base_data[base_data["record_type"].isin(["discharge_summary", "discharge_medication"])]
    
    # --- LABORATORY SPECIMEN MATRIX ASSEMBLY (FIXED SCHEMA RESHAPE) ---
    lab_final = pd.DataFrame()
    if not lab_src.empty:
        id_vars = ["patient_name", "uhid", "age", "gender", "hospital_name", "report_date", "source_file", "inserted_at", "is_flagged", "all_flags", "raw_json_unpacked"]
        pivot_src = lab_src.copy()
        
        pivot_src["range_str"] = pivot_src.apply(
            lambda r: f"{r.get('range_low') or ''} - {r.get('range_high') or ''}" if r.get("range_low") or r.get("range_high") else None, 
            axis=1
        )
        
        pivoted_list = []
        for _, grp in pivot_src.groupby(["patient_name", "report_date"], dropna=False):
            patient_row = {col: grp[col].iloc[0] for col in id_vars if col in grp.columns}
            patient_row["record_type"] = "lab_test"
            patient_row["id"] = grp["id"].iloc[0]
            
            # Formulate structured 5-column sub-arrays per distinct analyte target
            for test in defined_tests:
                patient_row[f"{test}"] = None
                patient_row[f"{test}_Result"] = None
                patient_row[f"{test}_Range"] = None
                patient_row[f"{test}_Unit"] = None
                patient_row[f"{test}_Analytics"] = None
            
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

    # --- HOSPITAL DISCHARGE LOG ASSEMBLY (STRUCTURAL ISOLATION) ---
    discharge_final = pd.DataFrame()
    if not discharge_src.empty:
        lab_columns_to_drop = ["test_name", "test_name_raw", "result_raw", "result_numeric", "is_numeric_result", "range_low", "range_high", "unit", "test_analytics"]
        discharge_final = discharge_src.drop(columns=lab_columns_to_drop, errors="ignore").dropna(axis=1, how="all")
        
    return lab_final, discharge_final


def main():
    st.title("Clinical Data Standardization Control Tower")
    st.markdown("Veritas Claims Infrastructure Governance Console")

    tab_records, tab_flagged, tab_analytics, tab_inspector, tab_upload = st.tabs([
        "Canonical Registries",
        "Operational Quality Queue",
        "Comparative Clinic Analytics",
        "Audit Lineage Inspector",
        "System Ingestion Endpoint"
    ])

    parser, standardizer, validator = get_pipeline_components()
    defined_tests = load_defined_tests()

    # Synchronize database records layer
    all_rows = DB.fetch_all()
    lab_df, discharge_df = records_to_dataframe(all_rows, defined_tests)
    df = pd.concat([lab_df, discharge_df], axis=0, ignore_index=True) if (not lab_df.empty or not discharge_df.empty) else pd.DataFrame()

    with tab_records:
        if lab_df.empty and discharge_df.empty:
            st.info("No standardized warehouse records located in the target database.")
        else:
            view_mode = st.radio(
                "Filter Target Domain Registry",
                ["Laboratory Diagnostic Reports", "Hospital Discharge Summaries"],
                horizontal=True
            )
            
            st.markdown("---")
            
            if view_mode == "Laboratory Diagnostic Reports":
                if lab_df.empty:
                    st.info("No normalized laboratory panels currently match the criteria.")
                else:
                    st.markdown("### Normalized Laboratory Panel Records")
                    clean_labs = lab_df.drop(columns=["all_flags", "is_flagged", "raw_json_unpacked", "selector_label"], errors="ignore")
                    st.dataframe(clean_labs, use_container_width=True, hide_index=True)
            else:
                if discharge_df.empty:
                    st.info("No clinical admission summaries or discharge medications match the criteria.")
                else:
                    st.markdown("### Standardized Hospital Discharge Summaries")
                    clean_discharge = discharge_df.drop(columns=["all_flags", "is_flagged", "raw_json_unpacked", "selector_label"], errors="ignore")
                    st.dataframe(clean_discharge, use_container_width=True, hide_index=True)

    with tab_flagged:
        st.markdown("### System Quality Validation Review Gate")
        if df.empty:
            st.info("Data verification queues are empty.")
        else:
            flagged_df = df[df["is_flagged"] == True]
            
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.metric("Anomalies Pending Review", len(flagged_df))
            with col_m2:
                clean_count = len(df) - len(flagged_df)
                st.metric("Clean Records Processed", clean_count)
            
            st.markdown("---")
            if not flagged_df.empty:
                display_flagged = flagged_df.drop(columns=["all_flags", "is_flagged", "raw_json_unpacked", "selector_label", "has_missing_field", "has_schema_error", "is_duplicate_entry"], errors="ignore")
                st.dataframe(display_flagged, use_container_width=True, hide_index=True)
            else:
                st.success("All processed files currently verify as compliant with structural data rules.")

    with tab_analytics:
        st.markdown("### Data Provider Quality Metrics Scorecard")
        if df.empty:
            st.info("Ingest source documents to populate data quality tracking matrices.")
        else:
            if "hospital_name" in df.columns and not df["hospital_name"].isna().all():
                df["has_missing_field"] = df["validation_flags"].apply(
                    lambda flags: any("MISSING_FIELD" in str(f) for f in flags) if isinstance(flags, (list, set, tuple)) else False
                )
                df["has_schema_error"] = df["standardization_flags"].apply(
                    lambda flags: any("UNMAPPED" in str(f) for f in flags) if isinstance(flags, (list, set, tuple)) else False
                )
                df["is_duplicate_entry"] = df["validation_flags"].apply(
                    lambda flags: "DUPLICATE_RECORD" in flags if isinstance(flags, (list, set, tuple)) else False
                )

                clinic_grp = df.groupby("hospital_name").agg(
                    total_rows=("inserted_at", "count"),
                    missing_fields=("has_missing_field", "sum"),
                    schema_errors=("has_schema_error", "sum"),
                    duplicates=("is_duplicate_entry", "sum")
                ).reset_index()

                clinic_grp["Duplicate Rate (%)"] = ((clinic_grp["duplicates"] / clinic_grp["total_rows"]) * 100).round(2)
                clinic_grp["Error Rate (%)"] = ((clinic_grp["schema_errors"] / clinic_grp["total_rows"]) * 100).round(2)
                clinic_grp["Missing-Field Rate (%)"] = ((clinic_grp["missing_fields"] / clinic_grp["total_rows"]) * 100).round(2)

                st.dataframe(
                    clinic_grp[["hospital_name", "total_rows", "Duplicate Rate (%)", "Error Rate (%)", "Missing-Field Rate (%)"]],
                    use_container_width=True, hide_index=True
                )
            else:
                st.warning("Metadata elements insufficient to calculate clinic comparative matrices.")

            st.markdown("---")
            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.markdown("### Distribution by Clinical File Category")
                st.bar_chart(df["record_type"].value_counts())
            with col_chart2:
                all_flags = [flag for flags in df["all_flags"] for flag in flags] if "all_flags" in df.columns else []
                if all_flags:
                    st.markdown("### Infrastructure System Flag Incidence")
                    st.bar_chart(pd.Series(all_flags).value_counts())
                else:
                    st.write("No diagnostic flags logged across current runs.")

    with tab_inspector:
        st.markdown("### Unified Record Integrity Audit Trail")
        if df.empty:
            st.info("Audit lineage pipelines unavailable; table contains no active row history.")
        else:
            df["selector_label"] = df.apply(lambda r: f"[{str(r.get('record_type')).upper()}] {r.get('patient_name')} - {r.get('report_date') or r.get('admission_date') or 'Unspecified'} (ID: {r.get('id')})", axis=1)
            
            selected_label = st.selectbox("Select Target Audit Row Context", options=df["selector_label"].unique())
            selected_record = df[df["selector_label"] == selected_label].iloc[0]
            
            col_transformed, col_raw = st.columns(2)
            
            with col_transformed:
                st.markdown("### Canonical Transformed Output Schema")
                transformed_clean = {k: v for k, v in selected_record.to_dict().items() if k not in ["selector_label", "raw_json_unpacked", "all_flags", "has_missing_field", "has_schema_error", "is_duplicate_entry"]}
                st.json(transformed_clean)
                
            with col_raw:
                st.markdown("### Original Input Raw Payload Audit Trail (FR-4.3)")
                if isinstance(selected_record.get("raw_json_unpacked"), dict) and selected_record["raw_json_unpacked"]:
                    st.json(selected_record["raw_json_unpacked"])
                else:
                    st.warning("Raw immutable stream footprint missing for selected database lineage row.")

    with tab_upload:
        st.markdown("### Ad-Hoc Data Pipeline Injection Port")
        uploaded_files = st.file_uploader("Select Clinical Stream Payload Bundles", type="json", accept_multiple_files=True)
        if uploaded_files and st.button("Execute Pipeline Processing Sequence"):
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
                    st.error(f"Ingestion interface failed processing payload segment {uploaded.name}: {str(e)}")

            if canonical_records:
                validated = validator.validate_batch(canonical_records)
                inserted = DB.insert_records(validated)
                flagged = sum(1 for v in validated if not v.is_clean)
                
                DB.log_run(
                    source_description=f"system_endpoint_batch_load_{files_processed}_nodes", 
                    row_count=inserted, 
                    flagged_count=flagged
                )
                st.success(f"Processing Complete. Logs Saved: {inserted} rows added. Anomalies Blocked/Tagged: {flagged}.")
                st.rerun()


if __name__ == "__main__":
    main()