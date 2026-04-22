import os
import time
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('POSTGRES_USER', 'admin')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'admin123')
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'olist')

def get_engine():
    conn_str = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    return create_engine(conn_str)

def log_job(engine, job_name, status, start_time, rows=0, error=None, context=None):
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO metadata.audit_jobs 
            (job_name, status, duration_seconds, rows_processed, error_message, context, end_time)
            VALUES (:name, :status, :duration, :rows, :error, :context, CURRENT_TIMESTAMP)
        """), {
            "name": job_name,
            "status": status,
            "duration": duration,
            "rows": rows,
            "error": str(error) if error else None,
            "context": str(context) if context else None
        })
