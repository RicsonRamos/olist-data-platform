import hashlib
import os
import shutil
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from pipeline.utils import get_engine, log_job

# Layer Paths
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
LANDING_DIR = os.path.join(DATA_DIR, 'landing')
BRONZE_DIR = os.path.join(DATA_DIR, 'bronze')
SILVER_DIR = os.path.join(DATA_DIR, 'silver')

RAW_SCHEMA = 'raw'

def calculate_file_hash(file_path):
    """Generate SHA-256 hash to ensure file-level idempotency."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def is_file_processed(engine, file_hash):
    """Check if file hash exists in the checkpoint table."""
    query = text("SELECT 1 FROM metadata.processed_files WHERE file_hash = :file_hash")
    with engine.connect() as conn:
        return conn.execute(query, {"file_hash": file_hash}).fetchone() is not None

def clean_column_name(col):
    return col.lower().replace("dataset", "").replace("dataset", "").strip("_")

def ingest_data():
    engine = get_engine()
    total_start_time = time.time()
    
    print(f"🚀 [INGESTION] Starting Incremental Load | {datetime.now().strftime('%H:%M:%S')}")
    
    # Ensure Metadata environment is ready
    setup_path = os.path.join(os.path.dirname(__file__), 'audit_setup.sql')
    with open(setup_path, 'r') as f:
        setup_sql = f.read()
    
    with engine.begin() as conn:
        conn.execute(text(setup_sql))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};"))

    files = [f for f in os.listdir(LANDING_DIR) if f.endswith(('.csv', '.zip'))]
    
    if not files:
        print("ℹ️ [INGESTION] No new files in landing zone.")
        return

    for file in files:
        file_path = os.path.join(LANDING_DIR, file)
        table_name = file.replace("_dataset.csv", "").replace(".csv", "")
        file_hash = calculate_file_hash(file_path)

        if is_file_processed(engine, file_hash):
            print(f"⏭️ [SKIP] {file} (already processed via hash {file_hash[:8]})")
            continue

        start_time = time.time()
        print(f"📦 [PROCESS] {file} -> table: {table_name}")
        
        try:
            # 1. Read Data
            df = pd.read_csv(file_path)
            
            # 2. Add Metadata & Standardize
            df.columns = [clean_column_name(col) for col in df.columns]
            df['_metadata_ingested_at'] = datetime.now()
            df['_metadata_source_file'] = file
            df['_metadata_file_hash'] = file_hash

            # 3. Layer: Silver (Parquet)
            os.makedirs(SILVER_DIR, exist_ok=True)
            df.to_parquet(os.path.join(SILVER_DIR, f"{table_name}.parquet"), index=False)

            # 4. Layer: Raw (PostgreSQL)
            df.to_sql(table_name, engine, schema=RAW_SCHEMA, if_exists='append', index=False)

            # 5. Checkpoint & Audit
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO metadata.processed_files (file_name, file_hash, row_count)
                    VALUES (:file_name, :file_hash, :row_count)
                """), {"file_name": file, "file_hash": file_hash, "row_count": len(df)})
            
            log_job(engine, f"ingest_{table_name}", "SUCCESS", start_time, rows=len(df))

            # 6. Layer: Bronze (Archive)
            partition_dir = os.path.join(BRONZE_DIR, datetime.now().strftime('%Y-%m-%d'))
            os.makedirs(partition_dir, exist_ok=True)
            shutil.move(file_path, os.path.join(partition_dir, file))
            
            print(f"✅ [SUCCESS] {table_name} ({len(df)} rows) in {round(time.time()-start_time, 2)}s")

        except Exception as e:
            log_job(engine, f"ingest_{table_name}", "FAILED", start_time, error=e)
            print(f"❌ [ERROR] {table_name}: {str(e)}")
            raise

    print(f"🏁 [INGESTION] Completed in {round(time.time()-total_start_time, 2)}s")

if __name__ == "__main__":
    ingest_data()
