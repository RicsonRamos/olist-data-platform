import os
import zipfile
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import time
from pipeline.utils import get_engine, log_job

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
RAW_SCHEMA = 'raw'

def clean_column_name(name):
    return name.strip().lower().replace(' ', '_').replace('.', '_')

def ingest_data():
    engine = get_engine()
    total_start_time = time.time()
    
    # Ensure raw schema and audit table exist
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS metadata;"))
        
        # Run audit setup if it exists
        setup_path = os.path.join(os.path.dirname(__file__), 'audit_setup.sql')
        if os.path.exists(setup_path):
            with open(setup_path, 'r') as f:
                conn.execute(text(f.read()))
        
        conn.commit()
    
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') or f.endswith('.zip')]
    
    for file in files:
        file_path = os.path.join(DATA_DIR, file)
        table_name = file.replace('.csv', '').replace('.zip', '').replace('_dataset', '')
        
        start_time = time.time()
        
        try:
            if file.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as z:
                    csv_name = z.namelist()[0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f)
            else:
                df = pd.read_csv(file_path)
                
            if len(df) == 0:
                raise ValueError(f"Empty dataset detected for {table_name}")
                
            df.columns = [clean_column_name(col) for col in df.columns]
            
            # Resilient Ingestion Loop
            for attempt in range(3):
                try:
                    with engine.begin() as conn:
                        table_exists = conn.execute(text(
                            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{RAW_SCHEMA}' AND table_name = '{table_name}')"
                        )).scalar()
                        conn.execute(text("SET lock_timeout = '5s'"))
                        if table_exists:
                            conn.execute(text(f"TRUNCATE TABLE {RAW_SCHEMA}.{table_name}"))
                    
                    df.to_sql(
                        table_name, engine, schema=RAW_SCHEMA, 
                        if_exists='append' if table_exists else 'replace', 
                        index=False, chunksize=10000, method='multi'
                    )
                    
                    log_job(engine, f"ingest_{table_name}", "SUCCESS", start_time, rows=len(df))
                    print(f"[INGESTION] table={table_name} status=success rows={len(df)}")
                    break
                    
                except OperationalError as e:
                    if "lock timeout" not in str(e).lower() or attempt == 2:
                        log_job(engine, f"ingest_{table_name}", "FAILED", start_time, error=e)
                        raise
                    time.sleep(2 ** attempt)
        except Exception as e:
            log_job(engine, f"ingest_{table_name}", "FAILED", start_time, error=e)
            print(f"[INGESTION] [ERROR] table={table_name} error={str(e)}")
            raise
    
    total_duration = round(time.time() - total_start_time, 2)
    print(f"[INGESTION] total_duration={total_duration}s")

if __name__ == "__main__":
    try:
        ingest_data()
        print("--- Ingestion completed successfully! ---")
    except Exception as e:
        print(f"--- Ingestion failed: {e} ---")
