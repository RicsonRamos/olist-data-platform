import subprocess
import time
import os
import sys

def run_command(command, cwd=None):
    print(f"Running: {command}")
    process = subprocess.Popen(command, shell=True, cwd=cwd)
    process.wait()
    if process.returncode != 0:
        print(f"Error executing: {command}")
        return False
    return True

def main():
    # 1. Start Docker
    print("--- 1. Starting Infrastructure (Docker) ---")
    if not run_command("docker compose up -d"):
        print("Failed to start Docker. Make sure Docker Desktop is running.")
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
        # sys.exit(1) # Continue anyway, maybe it works

    # 3. Ingestion
    print("--- 3. Running Data Ingestion (Python) ---")
    if not run_command(f"{sys.executable} scripts/ingestion.py"):
        print("Ingestion failed.")
        sys.exit(1)

    # 4. dbt Transformation
    print("--- 4. Running dbt Transformations ---")
    dbt_dir = "dbt_project"
    if not run_command("dbt run --profiles-dir .", cwd=dbt_dir):
        print("dbt run failed.")
        sys.exit(1)
    
    if not run_command("dbt test --profiles-dir .", cwd=dbt_dir):
        print("dbt tests failed.")
        sys.exit(1)

    print("\n--- Pipeline Completed Successfully! ---")
    print("Check the 'consumption' schema in your database for Power BI views.")

if __name__ == "__main__":
    main()
