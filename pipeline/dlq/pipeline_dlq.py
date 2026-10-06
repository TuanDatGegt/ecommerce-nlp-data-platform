## pipeline/dlq/pipeline_dlq.py
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from pipeline.storage.pipeline_storage import PipelineStorage

logger = logging.getLogger("pipeline_dlq")


class DeadLetterQueue:
    """
    Dead Letter Queue (DLQ) Handler.
    Quản lý và cách ly bản ghi lỗi, ghi file JSON trực tiếp vào data/dlq/<layer>/
    """

    def __init__(self, storage: PipelineStorage = None):
        self.storage = storage or PipelineStorage()

    def write_error(
        self, data, layer: str, source_file: str, error_message: str
    ) -> Path:
        """
        Ghi nhận thông tin lỗi và danh sách bản ghi không hợp lệ vào
        data/dlq/<layer>/.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        clean_source_name = Path(source_file).stem
        filename = f"{clean_source_name}_{timestamp}.json"
        relative_path = f"dlq/{layer}/{filename}"
        # Ép kiểu dữ liệu lỗi về dạng danh sách dicts
        records = []
        if isinstance(data, pd.DataFrame) and not data.empty:
            records = data.to_dict(orient="records")
        elif isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            records = data
        else:
            records = [{"raw_data": str(data)}]
        payload = {
            "layer": layer,
            "source_file": source_file,
            "error_message": str(error_message),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "record_count": len(records),
            "records": records,
        }
        full_path = self.storage.get_full_path(relative_path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, default=str)
        logger.warning(
            f"[DLQ] Đã ghi nhận {len(records)} bản ghi lỗi tại layer '{layer}':{full_path}"
        )

        return full_path

    def read_errors(self, layer: str) -> list[dict]:
        """Đọc toàn bộ báo cáo lỗi của một layer phục vụ kiểm tra/truy vết."""
        dlq_dir = self.storage.get_full_path(f"dlq/{layer}")
        if not dlq_dir.exists():
            return []
        error_files = list(dlq_dir.glob("*.json"))
        results = []
        for file_path in error_files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    results.append(json.load(f))
            except Exception as e:
                logger.error(f"[DLQ READ ERROR] Thất bại khi đọc file {file_path}:{e}")
        return results
