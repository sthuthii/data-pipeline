import os
import json
from fastapi import FastAPI, Request, Response
from google.cloud import storage

# Import entire pipeline ecosystem
from src.ingestion import Ingestion, IngestedRecord
from src.parser import Parser
from src.standardizer import Standardizer
from src.validator import Validator
from src.database import Database

app = FastAPI()
storage_client = storage.Client()

@app.post("/")
async def receive_eventarc_trigger(request: Request):
    """Intercepts Cloud Storage events and executes the full ingestion pipeline."""
    try:
        headers = request.headers
        event_type = headers.get("ce-type")
        
        if event_type == "google.cloud.storage.object.v1.finalized":
            body = await request.json()
            bucket_id = body.get("bucket")
            object_id = body.get("name")
            
            if bucket_id and object_id and object_id.endswith(".json"):
                gcs_uri = f"gs://{bucket_id}/{object_id}"
                print(f" Pipeline triggered for: {gcs_uri}")
                
                # 1. Ingestion Stage
                bucket = storage_client.bucket(bucket_id)
                blob = bucket.blob(object_id)
                raw_bytes = blob.download_as_bytes()
                
                ingestion_engine = Ingestion(source="gcs", gcs_bucket=bucket_id)
                ingested_record: IngestedRecord = ingestion_engine._process_bytes(
                    raw_bytes=raw_bytes,
                    source_file=gcs_uri,
                    source="gcs"
                )
                
                if ingested_record.is_duplicate:
                    print(f" Duplicate detected (Checksum: {ingested_record.checksum}). Skipping workflow.")
                    return Response(content="Duplicate file skipped.", status_code=200)
                    
                if not ingested_record.is_valid:
                    print(f" Invalid JSON Structure: {ingested_record.error_message}")
                    return Response(content="Malformed file structural format.", status_code=400)
                
                # 2. Parsing Stage (IngestedRecord -> list[RawRow])
                print(" Running Parser...")
                parser = Parser()
                raw_rows = parser.parse(ingested_record)
                
                # 3. Standardization & Validation Loop
                print(f" Processing {len(raw_rows)} extracted data rows...")
                standardizer = Standardizer()
                validator = Validator()
                
                validated_records = []
                flagged_count = 0
                
                for row in raw_rows:
                    # Standardize (RawRow -> CanonicalRecord)
                    canonical_record = standardizer.standardize(row)
                    
                    # Validate (CanonicalRecord -> ValidatedRecord)
                    validated_record = validator.validate(canonical_record)
                    validated_records.append(validated_record)
                    
                    # Check the clean status property on your ValidatedRecord
                    # New fixed line
                    if not validated_record.is_clean:
                        flagged_count += 1
                
                # 4. Database Persistence Stage (list[ValidatedRecord] -> BigQuery)
                print(" Committing records to warehouse layer...")
                db = Database()  # Ensure your target config defaults to BigQuery in production env
                inserted_count = db.insert_records(validated_records)
                
                # 5. Pipeline Run Metrics Logging
                db.log_run(
                    source_description=gcs_uri, 
                    row_count=len(validated_records), 
                    flagged_count=flagged_count
                )
                
                print(f" Execution Complete. Committed {inserted_count} rows. Flagged {flagged_count} issues.")
                return Response(content="Pipeline run completed flawlessly.", status_code=200)
                
        return Response(content="Event skipped.", status_code=200)
    except Exception as e:
        print(f" Pipeline Execution Failure: {str(e)}")
        return Response(content=f"Execution Error: {str(e)}", status_code=500)