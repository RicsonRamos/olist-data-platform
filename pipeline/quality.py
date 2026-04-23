import logging

logger = logging.getLogger(__name__)

class DataQuality:
    @staticmethod
    def check_schema_drift(df, expected_columns, table_name):
        """Detect if incoming data has more or fewer columns than expected."""
        current_columns = set(df.columns)
        expected_columns = set(expected_columns)
        
        drift = current_columns - expected_columns
        missing = expected_columns - current_columns
        
        if drift:
            logger.warning(f"⚠️ Schema Drift detected in {table_name}: New columns {drift}")
        if missing:
            logger.error(f"❌ Critical Schema Change in {table_name}: Missing columns {missing}")
            # In production, we might raise an exception here or quarantine the data
            return False
        return True

    @staticmethod
    def check_volume_anomaly(engine, table_name, current_count, threshold=0.5):
        """Detect if current ingestion volume is significantly different from historical average."""
        try:
            with engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text(f"SELECT AVG(row_count) FROM metadata.processed_files WHERE file_name LIKE '{table_name}%'")).scalar()
                
                if result and result > 0:
                    diff = abs(current_count - result) / result
                    if diff > threshold:
                        logger.warning(f"📉 Volume Anomaly in {table_name}: Current {current_count} vs Avg {round(result, 0)} (Diff: {round(diff*100, 2)}%)")
                        return False
        except Exception as e:
            logger.warning(f"Could not perform volume check for {table_name}: {e}")
        return True

    @staticmethod
    def check_freshness(engine, table_name, column_name, max_hours=24):
        """Check if the most recent date in the table is within acceptable range."""
        # This is a placeholder for real DB freshness checks
        pass
