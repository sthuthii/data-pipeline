# Veritas Claims Analytics: Clinical Data Pipeline & Quality Control Tower

This repository contains the architecture and implementation for an enterprise-grade, event-driven clinical data ingestion, processing, standardization, and analytics platform. The system is designed to handle multi-clinic medical feeds, validate structural formats, resolve vocabulary misalignments, and enforce physiological data integrity constraints before persisting records to a high-scale data warehouse.

---

## Architecture Overview

The platform uses a decoupled, event-driven microservices architecture optimized for automated scaling, reliability, and clear separation of concerns.

### Data Flow Diagram

```
[Raw Incoming File]
        │
        ▼ (Uploads to Cloud Storage)
┌────────────────────────────────┐
│  Google Cloud Storage Bucket   │
└───────────────┬────────────────┘
                │
                ▼ (Pub/Sub Event Notification)
┌────────────────────────────────┐
│        Google Eventarc         │
└───────────────┬────────────────┘
                │
                ▼ (HTTP POST Webhook Trigger)
┌────────────────────────────────┐
│      Cloud Run Worker          │
│  ┌──────────────────────────┐  │
│  │ 1. Ingestion Engine      │  │
│  │    (Deduplication/Hash)  │  │
│  ├──────────────────────────┤  │
│  │ 2. Parser Module         │  │
│  ├──────────────────────────┤  │
│  │ 3. Standardizer Engine   │  │
│  ├──────────────────────────┤  │
│  │ 4. Quality Validator     │  │
│  └────────────┬─────────────┘  │
└───────────────┼────────────────┘
                │
                ▼ (Structured Matrix Inserts)
┌────────────────────────────────┐
│     BigQuery Data Warehouse    │
└───────────────┬────────────────┘
                │
                ▼ (Secure Read API Queries)
┌────────────────────────────────┐
│   Streamlit Control Tower UI   │
└────────────────────────────────┘

```

### Core Engine Stages

The processing cycle operates on a formal data transformation sequence:

1. **Ingestion Layer (`src/ingestion.py`):** Acts as a structural gatekeeper. It reads the raw incoming byte stream, calculates an immutable SHA256 checksum, verifies structural JSON schema validity, and screens out duplicate updates via an execution store.
2. **Parsing Layer (`src/parser.py`):** Isolates schema-specific components based on record classifications, translating raw data into granular, unflattened internal row matrices (`RawRow`).
3. **Standardization Layer (`src/standardizer.py`):** Performs vocabulary alignment, clinical code translation, date normalization, unit uniformities, and raw type-casting using structural configurations.
4. **Validation Layer (`src/validator.py`):** Evaluates standardized components against strict mandatory constraints and diagnostic reference ranges to flag structural anomalies.
5. **Database Layer (`src/database.py`):** Manages connection caching, batch execution optimizations, and handles transactional loads to underlying persistent backends (SQLite for localized testing or BigQuery for absolute scaling).

---

## Core Component Directory Structure

```text
├── cloudbuild.yaml          # Google Cloud Build pipeline automation setup
├── dashboard.py             # Streamlit Enterprise Control Tower interface
├── pipeline.db              # Local SQLite deduplication store (development)
├── requirements.txt         # Project package dependencies manifest
├── config/                  # Standardization mapping rules
│   └── test_name_mapping.json
└── src/                     # Core system modules
    ├── database.py          # Data warehouse ingestion and execution backend
    ├── ingestion.py         # SHA256 deduplication and raw byte ingestion
    ├── parser.py            # Variant clinical record structure parsing
    ├── standardizer.py      # Schema value formatting and normalization
    ├── validator.py         # Business rule validation and anomaly flagging
    └── worker.py            # Eventarc listener and orchestration layer

```

---

## Detailed Component Specifications

### 1. Ingestion Layer (`src/ingestion.py`)

Responsible for reading target payloads from either structural local folders or Google Cloud Storage buckets.

* **Deduplication Strategy:** Rather than loading the database with concurrent records, this class calculates a unique SHA256 hash using the keys and values of the raw payload. It checks a transactional tracking state table (`processed_files`) to intercept previously processed inputs before launching downstream resources.
* **Return Mapping:** Wraps verified contents into an `IngestedRecord` container along with contextual properties such as tracking identifiers.

### 2. Parser Module (`src/parser.py`)

Decouples application requirements from specific clinical formats.

* **Logic Routing:** Identifies specific payload signatures (e.g., searching for expected arrays like `responseDetails`) and delegates extraction to domain-specific methods (such as `_parse_lab_report` or `_parse_discharge_summary`).
* **Output:** Emits a collection of unified `RawRow` objects, abstracting away differences in source structures.

### 3. Standardizer Engine (`src/standardizer.py`)

Transforms messy data fields into highly clean canonical layouts.

* **Vocabulary Alignment:** Resolves varying terminology using mapped translation tables.
* **Data Cleansing:** Implements reliable parsing helpers including standard date parsing, gender normalization (e.g., standardizing input codes to uniform representations), numeric casting safely resilient to string noise, and parsing range definitions into isolated boundaries.

### 4. Quality Validator (`src/validator.py`)

Enforces strict quality barriers on variables prior to analytics visibility.

* **Structural Assurance:** Assesses whether record types are complete by cross-checking mandatory schema keys.
* **Biological Boundary Verification:** Reviews lab values against defined parameters. If a clinical data boundary is broken, the row is preserved but marked with specific issue codes (`MISSING_FIELD`, `UNMAPPED_TEST`, `RANGE_VIOLATION`) so downstream analytics can filter it out.

### 5. Database Core Backend (`src/database.py`)

Maintains transactional interfaces between Python classes and external storage systems.

* **Hybrid Execution Modes:** Supports local SQLite pipelines for testing alongside optimized big-data streaming architectures via Google BigQuery API configurations.
* **Audit Tracking:** Exposes structural reporting methods (`log_run`) to save global performance counts, tracking total entries alongside flag frequencies per file execution.

### 6. Orchestration Layer (`src/worker.py`)

A fast, lightweight API server engineered using FastAPI to act as an un-buffered event consumer.

* **Cloud Integration:** Listens for HTTP POST calls forwarded by Google Eventarc whenever files arrive inside specific storage buckets.
* **System Execution Pipeline:** Unpacks the Cloud Storage event metadata, downloads raw file bytes, and guides them step-by-step through the validation process before running database batch inserts.

---

## Technology Stack & Operational Infrastructure

The platform leverages a fully decoupled, serverless, and event-driven architecture built on top of the Google Cloud ecosystem to guarantee high availability and horizontal scaling.

### 1. Core Core Application Layers
* **Language Runtime:** `Python 3.11`
* **API Orchestration Framework:** `FastAPI` 
* **Operational Dashboard:** `Streamlit` 
* **Data Matrix Transformations:** `Pandas` / `Pydantic` 

### 2. Managed Google Cloud Infrastructure
* **Compute Execution Engine:** `Google Cloud Run` 
* **Event Broker Routing:** `Google Eventarc` 
* **Object Landing Storage:** `Google Cloud Storage (GCS)` 
* **Asynchronous Buffer / DLQ:** `Google Cloud Pub/Sub`
* **Enterprise Analytics Warehouse:** `Google BigQuery`
* **Container Compilation Pipeline:** `Google Cloud Build`

---
## Deployment Guide

### Prerequisites

* Google Cloud SDK (gcloud CLI) initialized.
* Authorized project access to `veritas-claims-analytics`.
* Configured Container Registry permissions.

### Steps to Build and Deploy the Execution Engine

1. **Compile and Containerize with Cloud Build:**
Submit your code context to Cloud Build to package the worker application environment into an automated container image:
```bash
gcloud builds submit --config=cloudbuild.yaml .

```


2. **Deploy to Cloud Run:**
Launch the container into a managed Cloud Run instance within your designated data sovereignty region:
```bash
gcloud run deploy medical-dashboard \
    --image gcr.io/veritas-claims-analytics/medical-worker:latest \
    --region asia-southeast1

```


3. **Verify Deployment Health:**
Ensure that the target service boots without errors and confirms structural port initialization by reading the Cloud Run revision history logs:
```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=medical-dashboard AND NOT textPayload:_stcore" --limit=20 --format="value(textPayload)"

```



---

## Data Control Tower Dashboard UI

The `dashboard.py` module delivers a comprehensive, real-time analytics panel using Streamlit to monitor data quality indicators and track processing health across various clinical sources.

### Key Management Views

* **Canonical Registries:** Displays clean, structured patient records, separated into diagnostic logs and discharge files.
* **Operational Quality Queue:** Acts as an anomaly review dashboard. It separates compliant entries from problematic ones and highlights missing values or range issues.
* **Comparative Clinic Analytics:** Aggregates error, duplicate, and missing-field rates by clinic to flag underperforming input streams.
* **Audit Lineage Inspector:** Provides a clear lineage view by placing the raw source payload side-by-side with the final, standardized data row.

### Launching the Dashboard Interface

To execute the visualization platform locally or via Cloud Shell, run the following command:

```bash
streamlit run dashboard.py --server.port 8080

```
Navigate to the web preview console on port 8080 to access the management interface.
 


##  System Limitations & Technical Debt

While the current architecture cleanly orchestrates asynchronous clinical ingestion pipelines, several structural boundary constraints have been consciously accepted for this release iteration:

* Current Limitations
JSON Only: The system expects raw data as JSON. Legacy healthcare formats (like HL7v2 or DICOM) require an external pre-parsing layer.

* In-Memory Mappings: Translation dictionaries are loaded into container memory on startup. This is incredibly fast for hundreds of codes, but scaling to hundreds of thousands would require an external database or Redis cache.

* Static Health Thresholds: High/low safety ranges are fixed numbers. They don't automatically adjust based on combinations of a patient's age, sex, or medical history.

* Manual DLQ Re-runs: Bad or broken files are successfully isolated in a Dead-Letter Queue (DLQ), but fixing and re-processing them requires manual administrator intervention for now.

---
## Architectural Trade-offs
* BigQuery vs. Standard SQL: Slightly slower file-write speeds, but infinitely faster historical dashboard queries without any index maintenance.
* Cloud Run vs. Kubernetes: We sacrificed deep cluster networking tweaks for zero server management and an auto-scaling cloud bill that drops to $0$ when idle.
* JSON Configs vs. Database Lookups: Rule changes require a quick configuration file save instead of a live database cell edit, but it keeps our processing loop lightning-fast by avoiding network round-trips.
* Flagging vs. Dropping Data: We save and tag imperfect rows instead of rejecting them. This keeps some flagged entries in our tables, but it ensures an unbroken audit trail for administrators to troubleshoot.