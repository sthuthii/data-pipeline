 
## 1. Full-Fledged System Architecture Diagram


![Architecture Diagram](architecture.png)


## 2. Component Overviews



### 1. Ingestion Layer (The Front Gate)



* **Storage Landing Zone:** Clinics upload their raw patient JSON files directly into a secure, global Google Cloud Storage (GCS) bucket.

* **Event Ingress:** There are no manual timers or checking scripts. The system sits back and waits for Google Eventarc to notice when a new file hits the bucket.

* **Microservice Dispatches:** The moment a file lands, Eventarc automatically fires a lightweight webhook alert (HTTP POST) over to the processing engine.



### 2. Processing Layer (The Workhorse)



* **Stateless Orchestration:** Built with **FastAPI** running on **Google Cloud Run**, this handles incoming webhooks instantly and scales up automatically when traffic spikes.

* **Deduplication Check:** To prevent processing the same file twice, the system calculates a unique SHA256 fingerprint of the incoming data. If it matches an older run, it drops the file safely.

* **Smart Parsing:** The parser figures out what kind of document it is looking at (like a lab test or a hospital discharge list) and splits it into individual, raw rows.

* **Cleanup & Standardization:** The standardizer translates messy, clinic-specific terms into standard clinical language. It fixes varying gender codes, normalizes mixed-up date formats, and turns text results into clean numbers.

* **Quality Gates:** The validator runs the clean data against biological boundaries. If something looks off (like a missing field or a wild value), it doesn't delete the record; it just tags it with a warning (`MISSING_FIELD`, `RANGE_VIOLATION`) so it can still be tracked.



### 3. Storage Layer (The Memory)



* **Database Selection:** We use **Google BigQuery** for high-speed analysis on huge volumes of text data. It also supports a local **SQLite** database (`pipeline.db`) for isolated testing.

* **Clean Data Schema:** Cleansed data is stored in flat tables containing:

* **Lineage Keys:** Unique row IDs, source file paths, clinic names, and execution timestamps.

* **Category Codes:** Record labels identifying the row type (lab test, discharge medication, etc.).

* **Clean Payloads:** Relational columns mapped out neatly (names, ages, lab values, reference ranges).

* **Audit Flags:** Standardized status tags saved right alongside the columns for easy filtering.







### 4. Configuration Layer (Zero-Code Control)



* **Decoupled Maps:** All clinic conversion rules, dictionaries, and test names are saved completely outside the core app code in a standalone config file (`config/test_name_mapping.json`).

* **Live Adjustments:** When the pipeline spins up, the standardizer reads these mapping dictionaries directly from memory to match changing clinic keys to standard values.

* **Plug-and-Play Onboarding:** Because translation rules live in standard configuration files, you can onboard an entirely new clinic or adjust for schema shifts without changing a single line of Python code or rebuilding your containers.



### 5. Error Handling & Recoverability (The Safety Net)



* **Isolated Code Blocs:** Every processing step is wrapped inside a robust `try-except` bubble. A corrupted row or broken schema will throw a quiet log error instead of crashing the whole container or blocking other files.

* **Dead-Letter Queue (DLQ):** Backed by Cloud Pub/Sub, if a file causes repeated crashes or timeouts, the network safely isolates the message and moves it to a "Dead-Letter Topic" for admin review.

* **Live Alerts:** System logs are pushed directly to Google Cloud Monitoring. If the pipeline's error rates climb past 1%, or processing lags behind the SLA window, the system immediately triggers a page to administrators.



### 6. UI Layer (The Control Tower)



* **Dashboard Server:** An enterprise-focused management layout built in Python using **Streamlit** (`dashboard.py`).

* **Direct Warehouse View:** Connects securely to BigQuery, pulling data matrices directly into fast pandas dataframes for review.

* **Operational Layouts:**

* **Canonical Registries:** Separates clean, normalized data into distinct tabs for Lab Panels and Discharge Summaries.

* **Operational Quality Queue:** Targets flagged rows side-by-side with system error counters for tracking anomalies.

* **Comparative Clinic Analytics:** Aggregates data into live graphs to track error, duplicate, and missing-field rates by specific clinic.

* **Audit Lineage Inspector:** Places the original raw JSON right next to the finished database output so data stewards can trace exactly how a record was transformed.