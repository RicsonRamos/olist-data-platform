from sqlalchemy import text

from pipeline.utils import get_engine


class PipelineState:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class StateMachine:
    def __init__(self, run_id):
        self.run_id = run_id
        self.engine = get_engine()

    def transition_to(self, status, metadata=None):
        """Update the global run state in the database."""
        print(f"🔄 [STATE] Run {self.run_id} transitioning to {status}")

        query = text("""
            INSERT INTO metadata.pipeline_runs (run_id, status, metadata)
            VALUES (:run_id, :status, :metadata)
            ON CONFLICT (run_id) DO UPDATE 
            SET status = :status, 
                metadata = metadata.pipeline_runs.metadata || :metadata,
                end_time = CASE WHEN :status IN ('SUCCESS', 'FAILED') THEN CURRENT_TIMESTAMP ELSE NULL END
        """)

        with self.engine.begin() as conn:
            conn.execute(
                query,
                {
                    "run_id": self.run_id,
                    "status": status,
                    "metadata": metadata if metadata else {},
                },
            )
