## pipeline/silver/processing/cleaner.py
import re
import html
import logging
import pandas as pd

logger = logging.getLogger("silver_cleaner")

# Compile sẵn Regex patterns để tối ưu tốc độ thực thi
HTML_TAG_RE = re.compile(r"<[^>]+>")
MULTIPLE_SPACES_RE = re.compile(r"\s+")
SPECIAL_CHARS_RE = re.compile(r"[^\w\s.,!?\'-]")


def strip_html_tags(text: str) -> str:
    """Loại bỏ các thẻ HTML và decode HTML entities (vd: &amp; -&gt; &amp;)."""
    if not isinstance(text, str) or not text:
        return ""
    # Unescape HTML entities trước, sau đó xóa tags
    clean_text = html.unescape(text)
    clean_text = HTML_TAG_RE.sub(" ", clean_text)
    return clean_text


def normalize_text(text: str, lowercase: bool = True) -> str:
    """
    Tải và chuẩn hóa văn bản:
    1. Bóc tách HTML tags
    2. Chuyển về chữ thường (lowercase)
    3. Loại bỏ ký tự đặc biệt không mong muốn
    4. Gom nhóm khoảng trắng dư thừa
    """
    if not isinstance(text, str) or not text.strip():
        return ""

    cleaned = strip_html_tags(text)
    if lowercase:
        cleaned = cleaned.lower()

    cleaned = SPECIAL_CHARS_RE.sub(" ", cleaned)
    cleaned = MULTIPLE_SPACES_RE.sub(" ", cleaned).strip()
    return cleaned


def clean_review_dataframe(
    df: pd.DataFrame, text_columns: list[str] = None
) -> pd.DataFrame:
    """
    Áp dụng hàm chuẩn hóa văn bản cho toàn bộ DataFrame Pandas/PyArrow batch.
    """
    if text_columns is None:
        text_columns = ["review_headline", "review_body"]

    df = df.copy()
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(normalize_text)

    return df
