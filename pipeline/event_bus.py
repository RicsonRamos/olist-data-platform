import json
import uuid
import hashlib
from sqlalchemy import text
from pipeline.utils import get_engine

class EventBus:
    def __init__(self, run_id=None):
        self.engine = get_engine()
        self.run_id = run_id or str(uuid.uuid4())

    def emit(self, event_type, payload=None, partition_key=None, idempotency_key=None):
        """Persist event with deduplication and partitioning logic."""
        
        # Auto-generate idempotency key if not provided (stable for same type/payload/run)
        if not idempotency_key and payload:
            payload_str = json.dumps(payload, sort_keys=True)
            idempotency_key = hashlib.sha256(f"{event_type}{payload_str}{self.run_id}".encode()).hexdigest()

        print(f"📣 [EVENT] {event_type} | Partition: {partition_key} | IDKey: {idempotency_key[:8]}...")
        
        query = text("""
            INSERT INTO metadata.event_log (run_id, event_type, payload, partition_key, idempotency_key)
            VALUES (:run_id, :event_type, :payload, :partition_key, :idempotency_key)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING event_id
        """)
        
        with self.engine.begin() as conn:
            result = conn.execute(query, {
                "run_id": self.run_id,
                "event_type": event_type,
                "payload": json.dumps(payload) if payload else None,
                "partition_key": partition_key,
                "idempotency_key": idempotency_key
            }).fetchone()
            
            return result[0] if result else None
            
    def mark_processed(self, event_id, consumer_group, status="PROCESSED"):
        """Track processing status for specific consumer groups."""
        query = text("""
            INSERT INTO metadata.event_processing (event_id, consumer_group, status, processed_at)
            VALUES (:event_id, :consumer_group, :status, CURRENT_TIMESTAMP)
            ON CONFLICT (event_id, consumer_group) DO UPDATE SET status = :status, processed_at = CURRENT_TIMESTAMP
        """)
        with self.engine.begin() as conn:
            conn.execute(query, {"event_id": event_id, "consumer_group": consumer_group, "status": status})

    def get_history(self, event_type=None, partition_key=None):
        """Retrieve historical events with filtering."""
        query_str = "SELECT * FROM metadata.event_log WHERE 1=1"
        params = {}
        if event_type:
            query_str += " AND event_type = :event_type"
            params["event_type"] = event_type
        if partition_key:
            query_str += " AND partition_key = :partition_key"
            params["partition_key"] = partition_key
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query_str), params)
            return [dict(row._mapping) for row in result]
