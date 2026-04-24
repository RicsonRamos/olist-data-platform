import subprocess
import sys
import time


def run_command(command, cwd=None):
    """Execute shell commands and handle errors."""
    print(f"🛠️ Running: {command}")
    process = subprocess.Popen(command, shell=True, cwd=cwd)
    process.wait()
    if process.returncode != 0:
        raise Exception(f"❌ Command failed: {command}")


def start_infra():
    """Ensure database infrastructure is healthy."""
    print("🐳 Starting Infrastructure (Docker)...")
    run_command("docker compose up -d")

    # Healthcheck loop
    retries = 30
    for _i in range(retries):
        result = subprocess.run(
            "docker inspect --format='{{.State.Health.Status}}' olist_postgres",
            shell=True,
            capture_output=True,
            text=True,
        )
        if "healthy" in result.stdout:
            print("✅ Database is ready.")
            return
        time.sleep(2)
    raise Exception("❌ Database healthcheck timed out.")


def main():
    total_start_time = time.time()
    print("\n" + "=" * 50)
    print("📦 OLIST DATA PIPELINE: ORCHESTRATION START")
    print("=" * 50)

    try:
        # Step 1: Infrastructure
        start_infra()

        # Step 2: Incremental Ingestion (Raw, Bronze, Silver)
        run_command(f"{sys.executable} -m pipeline.ingestion")

        # Step 3: Warehouse Transformations (Gold Layer via dbt)
        print("💎 Running dbt Transformations...")
        run_command(f"{sys.executable} -m dbt.cli.main run --profiles-dir .", cwd="dbt")

        # Step 4: Quality Checks
        print("🧪 Running dbt Tests...")
        run_command(
            f"{sys.executable} -m dbt.cli.main test --profiles-dir .", cwd="dbt"
        )

        print("\n" + "=" * 50)
        print(
            f"🏁 PIPELINE COMPLETED SUCCESSFULLY in {round(time.time() - total_start_time, 2)}s"
        )
        print("=" * 50)

    except Exception as e:
        print(f"\n🚨 PIPELINE FAILED: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
