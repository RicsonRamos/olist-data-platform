import os
import time

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()

def get_engine():
    """Create a SQLAlchemy engine using environment variables."""
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB")
    
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str)

def log_job(engine, job_name, status, start_time, rows=0, error=None):
    """Log job execution details to the audit table."""
    duration = time.time() - start_time
    query = text("""
        INSERT INTO metadata.audit_jobs (job_name, status, duration_seconds, rows_processed, error_message)
        VALUES (:job_name, :status, :duration, :rows, :error)
    """)
    
    with engine.begin() as conn:
        conn.execute(query, {
            "job_name": job_name,
            "status": status,
            "duration": duration,
            "rows": rows,
            "error": str(error) if error else None
        })
