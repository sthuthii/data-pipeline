# Medical Data Standardization Pipeline

An end-to-end pipeline that ingests medical JSON documents (lab reports
and discharge summaries), standardizes them into a canonical schema,
validates the data, stores it, and visualizes results with Streamlit.

## Workflow

```
User Upload → (Local folder / Google Cloud Storage) → ingestion.py → parser.py
→ standardizer.py → validator.py → Canonical Record → BigQuery / SQLite → Streamlit Dashboard
```

## Project structure

```
medical-data-pipeline/
├── sample-data/                     # Sample source JSON documents
├── config/
│   ├── test_name_mapping.json       # Canonical lab test name ↔ variants
│   ├── medicine_mapping.json        # Canonical medicine name ↔ variants
│   ├── unit_mapping.json            # Canonical unit ↔ variants
│   └── reference_ranges.json        # Standard clinical reference ranges
├── src/
│   ├── ingestion.py                 # Read + validate raw JSON (local or GCS)
│   ├── parser.py                    # Detect doc type, flatten to raw rows
│   ├── standardizer.py              # Normalize names/units/dates/gender
│   ├── validator.py                 # Mandatory fields, ranges, duplicates
│   ├── database.py                  # SQLite (default) or BigQuery storage
│   └── dashboard.py                 # Streamlit UI
├── main.py                          # CLI pipeline runner
├── requirements.txt
├── .env.example
└── pipeline_output.db               # created after first run (sqlite)
```

## Quick start (local, no GCP needed)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run the full pipeline against sample-data/, writing to a local SQLite file
python main.py

# Explore the results in a browser
streamlit run src/dashboard.py
```

Each module also runs standalone for quick debugging, e.g.:

```bash
python -m src.ingestion
python -m src.parser
python -m src.standardizer
python -m src.validator
```

## Moving to GCP (Cloud Storage + BigQuery)

1. Copy `.env.example` to `.env` and fill in `GCP_PROJECT_ID`,
   `GCS_BUCKET_NAME`, `BQ_DATASET`, `BQ_TABLE`, and point
   `GOOGLE_APPLICATION_CREDENTIALS` at a service account key.
2. Install the optional GCP dependencies (uncomment them in
   `requirements.txt`):
   ```bash
   pip install google-cloud-storage google-cloud-bigquery
   ```
3. Create the BigQuery dataset/table (schema mirrors
   `database.py::_insert_bigquery`'s row shape - `record_type`,
   `source_file`, `document_id`, `correlation_id`,
   `canonical_fields_json`, `standardization_flags`,
   `validation_flags`, `inserted_at`).
4. Run:
   ```bash
   python main.py --source gcs --backend bigquery \
       --gcs-bucket $GCS_BUCKET_NAME --bq-project $GCP_PROJECT_ID
   ```
5. Deploy `src/dashboard.py` to Cloud Run for a hosted dashboard; wire
   Cloud Logging into the existing `logging` calls for centralized logs.

## Design notes / how the mapping configs were built

The `config/test_name_mapping.json` file was seeded from a canonical
test-name reference sheet (grouping raw variants like `HAEMOGLOBIN`,
`Haemoglobin`, `Hb` under one canonical `HEMOGLOBIN` key) plus the OCR-
truncated names observed in the sample lab report (`aemoglobin`,
`ematocrit HCT`, `eutrophils`, etc. — missing leading characters).
Because that kind of truncation is common with OCR/extraction pipelines,
`standardizer.py`'s `NameLookup` does an exact match first and falls back
to `difflib`-based fuzzy matching (tunable via `FUZZY_MATCH_THRESHOLD`)
so new truncated variants don't automatically fall through as unmapped.

`config/medicine_mapping.json` maps hospital-specific brand/short names
(`Inj. Pan`, `CAP. PAN 40`, `INJ PANTOP 40 MG`) to a single generic drug
name (`PANTOPRAZOLE`) with a drug category, based on the medication
names present in the two sample discharge summaries.

`config/reference_ranges.json` uses general adult clinical reference
ranges for the validator's out-of-range check — these are reasonable
defaults for a demo/portfolio project, not a substitute for lab-specific
or age/sex-adjusted ranges in a real clinical setting.

## Known limitations / next steps

- Age fields in the sample data are redacted (`[AGE REDACTED]`), so
  age-based reference range adjustment isn't implemented yet — currently
  reference ranges are adult general-population defaults regardless of
  age/gender.
- Reference ranges are not gender/age-adjusted; that would be a natural
  next enhancement now that gender is normalized.
- `database.py`'s BigQuery path is written but untested against a live
  project — validate the schema and permissions before first real run.
- No retry/backoff logic on GCS or BigQuery calls yet.
- Duplicate detection is per-batch (in-memory) rather than checked
  against everything already stored in BigQuery/SQLite.
