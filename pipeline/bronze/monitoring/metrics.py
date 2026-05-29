#utils/metrics.py
import time
from datetime import datetime, timezone

class PinelineMetrics:
    def __init__(self):
        self.pipeline_start_time = time.time()
        self.total_rows = 0
        self.failed_rows = 0
        self.processed_files = 0
        self.processed_chunks = 0
        self.start_datetime = datetime.now(timezone.utc).isoformat()
    

    def add_rows(self, rows: int):
        self.total_rows += rows
    
    def add_failed_rows(self, rows: int):
        self.failed_rows += rows

    def add_processed_file(self):
        self.processed_files += 1

    def add_processed_chunk(self):
        self.processed_chunks += 1

    def summary(self):
        duration = time.time() - self.pipeline_start_time

        return {
            f"pipeline_start_time": self.start_datetime,
            f"processed_files": self.processed_files,
            f"processed_chunks": self.processed_chunks,
            f"processed_rows": self.total_rows,
            f"failed_rows": self.failed_rows,
            f"duration_seconds": round(duration, 2)
        }
    
    def print_summary(self):
        summary = self.summary()
        print("\n======== PIPELINE METRICS SUMMARY ========")
        
        for key, value in summary.items():
            print(f"{key}: {value}")

        print("==========================================\n")