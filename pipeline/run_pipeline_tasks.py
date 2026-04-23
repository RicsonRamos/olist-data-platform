import subprocess
import time
import os
import sys
from pipeline.utils import get_engine, log_job

def run_command(command, cwd=None):
    process = subprocess.Popen(command, shell=True, cwd=cwd)
    process.wait()
    if process.returncode != 0:
        raise Exception(f"Command failed: {command}")

def start_infra():
    run_command("docker compose up -d")
    retries = 30
    ready = False
    for i in range(retries):
        result = subprocess.run("docker inspect --format='{{.State.Health.Status}}' olist_postgres", 
                                shell=True, capture_output=True, text=True)
        if "healthy" in result.stdout:
            ready = True
            break
        time.sleep(2)
    if not ready:
        raise Exception("Database healthcheck timed out.")

def run_ingestion():
    run_command(f"{sys.executable} -m pipeline.ingestion")

def run_dbt_transformations():
    engine = get_engine()
    dbt_dir = "dbt"
    start_time = time.time()
    try:
        run_command(f"{sys.executable} -m dbt.cli.main run --profiles-dir .", cwd=dbt_dir)
        log_job(engine, "dbt_run", "SUCCESS", start_time)
    except Exception as e:
        log_job(engine, "dbt_run", "FAILED", start_time, error=e)
        raise

def run_dbt_tests():
    engine = get_engine()
    dbt_dir = "dbt"
    start_time = time.time()
    try:
        run_command(f"{sys.executable} -m dbt.cli.main test --profiles-dir .", cwd=dbt_dir)
        log_job(engine, "dbt_test", "SUCCESS", start_time)
    except Exception as e:
        log_job(engine, "dbt_test", "FAILED", start_time, error=e)
        raise
