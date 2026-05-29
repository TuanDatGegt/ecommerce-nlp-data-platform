#pipeline/bronze/processing/metadata.py

import os
import re
from datetime import datetime, timezone

from configs.settings import CATEGORY_MAPPING


def extract_category_from_filename(file_name: str) ->str:
    match = re.search(CATEGORY_MAPPING, file_name)

    if match:
        return match.group(1)

    return 'Unknown'


def build_partition_prefix(category: str):
    now = datetime.now(timezone.utc)
    year = now.year

    month = f"{now.month:02d}"

    return os.path.join(
        "bronze",
        "reviews",
        f"year={year}",
        f"month={month}",
        f"category={category}"
    )

