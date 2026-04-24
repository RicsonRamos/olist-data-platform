from prefect import flow, task
from pipeline.run_pipeline_tasks import start_infra, run_ingestion, run_dbt_transformations, run_dbt_tests

@task(retries=3, retry_delay_seconds=10)
def task_infra():
    """Ensure database infrastructure is healthy."""
    start_infra()

@task
def task_ingestion():
    """Run incremental ingestion (Raw/Bronze/Silver)."""
    run_ingestion()

@task
def task_transformations():
    """Run dbt transformations (Gold layer)."""
    run_dbt_transformations()

@task
def task_tests():
    """Run data quality tests."""
    run_dbt_tests()

@flow(name="Olist Data Platform DAG")
def olist_dag():
    """Main Orchestration Flow for Olist Data Platform."""
    infra = task_infra()
    ingest = task_ingestion(wait_for=[infra])
    transform = task_transformations(wait_for=[ingest])
    task_tests(wait_for=[transform])

if __name__ == "__main__":
    olist_dag()
