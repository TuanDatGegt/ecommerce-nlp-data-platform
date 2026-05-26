#utils/metrics.py
import time

class PinelineMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.total_rows = 0
        self.failed_rows = 0
        self.processed_files = 0

    def add_rows(self, n):
        self.total_rows += n
    
    def add_failed_rows(self, n):
        self.failed_rows += n

    def add_file(self):
        self.processed_files += 1

    def summary(self):
        duration = time.time() - self.start_time

        return {
            f"processed_files": self.processed_files,
            f"processed_rows": self.total_rows,
            f"failed_rows": self.failed_rows,
            f"duration_seconds": round(duration, 2)
        }
    
    