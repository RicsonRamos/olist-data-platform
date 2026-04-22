import os
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

DB_USER = os.getenv('POSTGRES_USER', 'admin')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'admin123')
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'olist')

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
RAW_SCHEMA = 'raw'

from sqlalchemy.exc import OperationalError
import time

def get_engine():
    conn_str = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    return create_engine(conn_str)

def clean_column_name(name):
    return name.strip().lower().replace(' ', '_').replace('.', '_')

def ingest_data():
    engine = get_engine()
    total_start_time = time.time()
    
    # Ensure raw schema exists
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};"))
        conn.commit()
    
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') or f.endswith('.zip')]
    
    for file in files:
        file_path = os.path.join(DATA_DIR, file)
        table_name = file.replace('.csv', '').replace('.zip', '').replace('_dataset', '')
        
        start_time = time.time()
        
        if file.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as z:
                # Assume one CSV per zip
                csv_name = z.namelist()[0]
                with z.open(csv_name) as f:
                    df = pd.read_csv(f)
        else:
            df = pd.read_csv(file_path)
            
        # Validation: Empty dataset check
        if len(df) == 0:
            raise ValueError(f"Empty dataset detected for {table_name}")
            
        # Basic cleaning
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Resilient Ingestion Loop
        for attempt in range(3):
            try:
                with engine.begin() as conn:
                    # Check if table exists
                    table_exists = conn.execute(text(
                        f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{RAW_SCHEMA}' AND table_name = '{table_name}')"
                    )).scalar()
                    
                    # Lock management and Truncate
                    conn.execute(text("SET lock_timeout = '5s'"))
                    if table_exists:
                        conn.execute(text(f"TRUNCATE TABLE {RAW_SCHEMA}.{table_name}"))
                
                # Load to SQL with Performance Optimization
                df.to_sql(
                    table_name, 
                    engine, 
                    schema=RAW_SCHEMA, 
                    if_exists='append' if table_exists else 'replace', 
                    index=False,
                    chunksize=10000,
                    method='multi'
                )
                
                duration = round(time.time() - start_time, 2)
                print(f"[INGESTION] table={table_name} status=success rows={len(df)} duration={duration}s")
                break
                
            except OperationalError as e:
                if "lock timeout" not in str(e).lower() or attempt == 2:
                    print(f"[INGESTION] [ERROR] table={table_name} error={str(e)}")
                    raise
                
                wait_time = 2 ** attempt
                print(f"[INGESTION] [RETRY] table={table_name} attempt={attempt+1} status=lock_detected wait={wait_time}s")
                time.sleep(wait_time)
            except Exception as e:
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
