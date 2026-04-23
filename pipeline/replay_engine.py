
class ReplayEngine:
    def __init__(self, event_bus, processor):
        self.bus = event_bus
        self.processor = processor

    def replay_entity(self, table_name, date_from=None):
        """Selective Replay: Process history only for a specific table/partition."""
        print(f"🕵️ [REPLAY ENGINE] Scouting history for entity: {table_name}")
        
        # Filter by partition_key (simulating entity partitioning)
        history = self.bus.get_history(event_type="FILE_ARRIVED", partition_key=table_name)
        
        if date_from:
            history = [e for e in history if e['created_at'].strftime('%Y-%m-%d') >= date_from]

        if not history:
            print(f"ℹ️ No historical events found for {table_name}.")
            return

        print(f"🚀 Selective Replay: Processing {len(history)} events for {table_name}...")
        for event in history:
            # We emit with a new run_id (inherent in the processor) but keep the partition
            self.processor.process_event(
                "FILE_ARRIVED", 
                payload=event['payload'], 
                partition_key=table_name
            )

    def replay_failed_events(self, consumer_group="default_worker"):
        """Checkpoint Replay: Re-process events that failed for a specific consumer group."""
        print(f"🛠️ [REPLAY ENGINE] Checking failures for {consumer_group}...")
        
        # This would require a more complex query joining event_log and event_processing
        # Placeholder for checkpoint logic
        pass
