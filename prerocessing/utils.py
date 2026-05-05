import re
from config import CATEGORY_MAPPING

def data_extract_category(file_name: str) ->str:
    match = re.search(CATEGORY_MAPPING, file_name)
    if match:
        return match.group(1)
    return 'Unknown'