import subprocess
import time
import os
import sys
from scripts.utils import get_engine, log_job

def run_command(command, cwd=None):
    print(f"Running: {command}")
    process = subprocess.Popen(command, shell=True, cwd=cwd)
    process.wait()
    if process.returncode != 0:
        print(f"Error executing: {command}")
        return False
    return True

def main():
    engine = get_engine()
    
    # 1. Start Infrastructure (Docker)
    print("--- 1. Starting Infrastructure (Docker) ---")
    max_retries = 3
    for attempt in range(max_retries):
        if run_command("docker compose up -d"):
            break
        if attempt < max_retries - 1:
            print(f"Docker startup failed. Retrying in 5s... ({attempt + 1}/{max_retries})")
            time.sleep(5)
        else:
            print("Failed to start Docker after multiple attempts. Check your internet connection or Docker Desktop status.")
            sys.exit(1)

    # 2. Wait for Postgres Healthcheck
    print("--- 2. Waiting for Database to be ready ---")
    retries = 30
    ready = False
    for i in range(retries):
        result = subprocess.run("docker inspect --format='{{.State.Health.Status}}' olist_postgres", 
                                shell=True, capture_output=True, text=True)
        if "healthy" in result.stdout:
            ready = True
            break
        print(f"Waiting... ({i+1}/{retries})")
        time.sleep(2)
    
    if not ready:
        print("Database healthcheck timed out.")

    # 3. Ingestion
    print("--- 3. Running Data Ingestion (Python) ---")
    # Ingestion logs its own internal jobs
    if not run_command(f"{sys.executable} scripts/ingestion.py"):
        print("Ingestion failed.")
        sys.exit(1)

    # 4. dbt Transformation
    print("--- 4. Running dbt Transformations ---")
    dbt_dir = "dbt_project"
    
    # dbt run
    start_time = time.time()
    if run_command("dbt run --profiles-dir .", cwd=dbt_dir):
        log_job(engine, "dbt_run", "SUCCESS", start_time)
    else:
        log_job(engine, "dbt_run", "FAILED", start_time)
        print("dbt run failed.")
        sys.exit(1)
    
    # dbt test
    start_time = time.time()
    if run_command("dbt test --profiles-dir .", cwd=dbt_dir):
        log_job(engine, "dbt_test", "SUCCESS", start_time)
    else:
        log_job(engine, "dbt_test", "FAILED", start_time)
        print("dbt tests failed.")
        sys.exit(1)

    print("\n--- Pipeline Completed Successfully! ---")
    print("Check the 'consumption' schema in your database for Power BI views.")

if __name__ == "__main__":
    main()
