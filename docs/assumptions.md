# Project Assumptions and Scope

## 1. Business Assumptions

### Clinic Data Upload

We assume clinics can upload their data directly to Google Cloud Storage
(GCS) using their existing systems, such as SFTP, cloud SDKs, or
Hospital Information Systems (HIS). Because of this, our pipeline does
not need to schedule or fetch files from clinics.

### Data Arrival

Clinic files are expected to arrive throughout the day instead of all at
once. On average, the system is designed to handle about **200,000 files
per day** (around **2.3 files per second**) and can automatically scale
to manage temporary spikes in traffic.

## 2. Technical Assumptions

### Data Storage

The amount of medical data is expected to grow significantly over time.
We use **Google BigQuery** because it is built for handling very large
datasets efficiently without worrying about server management, indexing,
or database scaling.

## 3. Data Assumptions

### Valid JSON Files

All incoming files are expected to be valid JSON. If a file is
incomplete or incorrectly formatted, it will fail validation during the
ingestion stage.

### Unique Record Identification

Each record should contain a unique identifier, such as `report_id` or
`documentId`. If one is missing, the system automatically creates a
unique ID using the file's checksum to maintain data traceability.

### Standardized Test Names

Different clinics may use different spellings or abbreviations for the
same test. The system uses a predefined mapping file
(`test_name_mapping.json`) to convert these variations into a standard
format.

## 4. Scope Exclusions

### User Login and Role-Based Access

**Excluded:** User authentication and role-based access control (RBAC).

**Reason:** The dashboard is designed as an internal administrative tool
rather than a multi-user application.

**To include:** Integrate an identity provider (such as Google Identity
Platform or Okta) and implement user roles and access permissions.

------------------------------------------------------------------------

### Patient Data Masking

**Excluded:** Automatic masking or encryption of sensitive patient
information.

**Reason:** This requires additional security infrastructure and
encryption key management.

**To include:** Add data masking during processing and integrate a
secure cloud key management service before storing data.

------------------------------------------------------------------------

### Automated Dead-Letter Queue (DLQ)

**Excluded:** Automatic retry of failed files.

**Reason:** Failed records often need manual review before they can be
safely processed again.

**To include:** Build an error management system with an admin interface
for reviewing, correcting, and resubmitting failed records.
