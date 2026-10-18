## pipeline/silver/processing/cleaner.py
import re
import html
import logging

import pandas as pd
import pyarrow as pa

logger = logging.getLogger("silver_cleaner")

HTML_TAG_RE = re.compile(r"<[^>]+>")
MULTIPLE_SPACES_RE = re.compile(r"\s+")
SPECIAL_CHARS_RE = re.compile(r"[^\w\s.,!?\'-]")

DEFAULT_TEXT_COLUMNS = ["review_headline", "review_body"]


def strip_html_tags(text: str) -> str:
    """Loại bỏ thẻ HTML và decode HTML entities (vd: &amp; -> &)."""
    if not isinstance(text, str) or not text:
        return ""
    return HTML_TAG_RE.sub(" ", html.unescape(text))


def normalize_text(text: str, lowercase: bool = True) -> str:
    """Bóc HTML -> lowercase -> bỏ ký tự đặc biệt -> gộp khoảng trắng. None/NaN -> ''."""
    if not isinstance(text, str) or not text.strip():
        return ""
    cleaned = strip_html_tags(text)
    if lowercase:
        cleaned = cleaned.lower()
    cleaned = SPECIAL_CHARS_RE.sub(" ", cleaned)
    return MULTIPLE_SPACES_RE.sub(" ", cleaned).strip()


def clean_review_table(table: pa.Table, text_columns: list[str] = None) -> pa.Table:
    """Làm sạch trực tiếp trên Arrow Table (không đi qua pandas => không lệch dtype, ít RAM)."""
    for col in text_columns or DEFAULT_TEXT_COLUMNS:
        if col in table.column_names:
            idx = table.column_names.index(col)
            cleaned = pa.array(
                [normalize_text(v) for v in table.column(idx).to_pylist()],
                type=pa.string(),
            )
            table = table.set_column(idx, col, cleaned)
    return table


def clean_review_dataframe(
    df: pd.DataFrame, text_columns: list[str] = None
) -> pd.DataFrame:
    """Giữ lại cho code cũ. Dùng .map thay vì .astype(str) để None không thành chuỗi 'none'."""
    df = df.copy()
    for col in text_columns or DEFAULT_TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].map(normalize_text)
    return df
