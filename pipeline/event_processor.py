from pipeline.orchestrator import Orchestrator
from pipeline.state_machine import PipelineState


class EventProcessor:
    def __init__(self, event_bus, state_machine, consumer_group="default_worker"):
        self.bus = event_bus
        self.state = state_machine
        self.consumer_group = consumer_group
        self.orch = Orchestrator()

    def process_event(self, event_type, payload=None, partition_key=None):
        """React to events with Deduplication & Exactly-once semantics."""

        # 1. Emit/Register Event (Handles IDKey automatically)
        event_id = self.bus.emit(event_type, payload, partition_key=partition_key)

        if not event_id:
            print(
                f"🚫 [DEDUP] Event {event_type} already registered. Skipping execution."
            )
            return

        # 2. Mark as Received for this Consumer Group
        self.bus.mark_processed(event_id, self.consumer_group, status="RECEIVED")

        # 3. Reactive Logic
        try:
            if event_type == "FILE_ARRIVED":
                self._handle_file_arrival(payload)

            # 4. Finalize - Mark as Processed
            self.bus.mark_processed(event_id, self.consumer_group, status="PROCESSED")

        except Exception as e:
            self.bus.mark_processed(event_id, self.consumer_group, status="FAILED")
            raise e

    def _handle_file_arrival(self, payload):
        from pipeline.run_pipeline_tasks import (
            run_dbt_tests,
            run_dbt_transformations,
            run_ingestion,
        )

        self.state.transition_to(PipelineState.RUNNING)

        try:
            self.orch.add_task("Ingestion", run_ingestion)
            self.orch.add_task(
                "Transformations", run_dbt_transformations, dependencies=["Ingestion"]
            )
            self.orch.add_task("Tests", run_dbt_tests, dependencies=["Transformations"])

            self.orch.run_all()

            if all(t.status == "SUCCESS" for t in self.orch.tasks.values()):
                self.state.transition_to(PipelineState.SUCCESS)
                self.bus.emit("PIPELINE_COMPLETED")
            else:
                self.state.transition_to(PipelineState.FAILED)
                self.bus.emit("PIPELINE_FAILED")

        except Exception as e:
            self.state.transition_to(PipelineState.FAILED, metadata={"error": str(e)})
            self.bus.emit("PIPELINE_FAILED", payload={"error": str(e)})
