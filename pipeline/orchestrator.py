import time
import logging
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Task:
    def __init__(self, name, func, dependencies=None, retries=3, retry_delay=5):
        self.name = name
        self.func = func
        self.dependencies = dependencies or []
        self.retries = retries
        self.retry_delay = retry_delay
        self.status = "PENDING"  # PENDING, RUNNING, SUCCESS, FAILED
        self.start_time = None
        self.end_time = None
        self.error = None

    def run(self, *args, **kwargs):
        self.status = "RUNNING"
        self.start_time = time.time()
        attempt = 0
        
        while attempt <= self.retries:
            try:
                logger.info(f"🚀 Executing task: {self.name} (Attempt {attempt + 1}/{self.retries + 1})")
                result = self.func(*args, **kwargs)
                self.status = "SUCCESS"
                self.end_time = time.time()
                logger.info(f"✅ Task {self.name} completed successfully.")
                return result
            except Exception as e:
                attempt += 1
                self.error = str(e)
                logger.error(f"❌ Task {self.name} failed: {e}")
                if attempt <= self.retries:
                    logger.info(f"Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                else:
                    self.status = "FAILED"
                    self.end_time = time.time()
                    raise

class Orchestrator:
    def __init__(self):
        self.tasks = {}
        self.execution_log = []

    def add_task(self, name, func, dependencies=None, retries=3):
        task = Task(name, func, dependencies, retries)
        self.tasks[name] = task
        return task

    def run_all(self):
        start_time = time.time()
        logger.info("🎬 Starting Pipeline Orchestration...")
        
        # In a real DAG, we'd sort by dependencies. For this simulation, 
        # we'll run in the order they were added, checking status.
        for name, task in self.tasks.items():
            # Check dependencies
            for dep in task.dependencies:
                if self.tasks[dep].status != "SUCCESS":
                    logger.error(f"🚫 Cannot run {name}: Dependency {dep} failed or not run.")
                    task.status = "SKIPPED"
                    continue
            
            if task.status == "PENDING":
                try:
                    task.run()
                except Exception:
                    logger.error(f"🚨 Pipeline halted due to critical failure in {name}")
                    break

        total_duration = time.time() - start_time
        self._print_summary(total_duration)

    def _print_summary(self, duration):
        summary = {
            "pipeline_status": "COMPLETED" if all(t.status in ["SUCCESS", "SKIPPED"] for t in self.tasks.values()) else "FAILED",
            "total_duration_seconds": round(duration, 2),
            "tasks": {name: t.status for name, t in self.tasks.items()}
        }
        print("\n" + "="*50)
        print("🏁 ORCHESTRATION SUMMARY")
        print("="*50)
        print(json.dumps(summary, indent=4))
        print("="*50)
