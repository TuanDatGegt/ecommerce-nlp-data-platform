## tests/test_storage_dlq.py
import pytest
import pandas as pd
from pathlib import Path
from pipeline.storage import PipelineStorage
from pipeline.dlq import DeadLetterQueue


@pytest.fixture
def temp_storage(tmp_path: Path) -> PipelineStorage:
    """Fixture tạo đối tượng PipelineStorage sử dụng thư mục tạm thời."""
    base_dir = tmp_path / "data"
    return PipelineStorage(base_dir=str(base_dir))


@pytest.fixture
def dlq_handler(temp_storage: PipelineStorage) -> DeadLetterQueue:
    """Fixture tạo đối tượng DeadLetterQueue gắn liền với temp_storage."""
    return DeadLetterQueue(storage=temp_storage)


# =====================================================================
# 1. KIỂM THỬ PIPELINE STORAGE
# =====================================================================


def test_storage_init_directories(temp_storage: PipelineStorage):
    """Kiểm tra xem các thư mục layer cơ bản đã được tự động khởi tạo chưa."""
    for layer in ["bronze", "silver", "gold", "dlq", "metadata"]:
        assert (temp_storage.base_dir / layer).exists()
        assert (temp_storage.base_dir / layer).is_dir()


def test_save_and_read_parquet(temp_storage: PipelineStorage):
    """Kiểm tra tính năng ghi và đọc lại file Parquet."""
    df_sample = pd.DataFrame(
        {
            "review_id": ["R1", "R2"],
            "star_rating": [5, 1],
            "category": ["Books", "Electronics"],
        }
    )

    relative_path = "bronze/reviews/year=2026/month=10/test_part.parquet"

    # 1. Thao tác ghi
    saved_path = temp_storage.save_parquet(df_sample, relative_path)
    assert saved_path.exists()
    assert saved_path.is_file()

    # 2. Thao tác kiểm tra tồn tại &amp; đọc
    assert temp_storage.exists(relative_path) is True

    df_read = temp_storage.read_parquet(relative_path)
    assert len(df_read) == 2
    assert list(df_read.columns) == ["review_id", "star_rating", "category"]
    assert df_read["review_id"].tolist() == ["R1", "R2"]


def test_list_and_delete_files(temp_storage: PipelineStorage):
    """Kiểm tra việc liệt kê danh sách file và xóa file."""
    df_dummy = pd.DataFrame({"col": [1, 2]})

    path1 = "bronze/test_list/file1.parquet"
    path2 = "bronze/test_list/file2.parquet"

    temp_storage.save_parquet(df_dummy, path1)
    temp_storage.save_parquet(df_dummy, path2)

    # Kiểm tra list_files
    files = temp_storage.list_files("bronze/test_list", pattern="*.parquet")
    assert len(files) == 2

    # Kiểm tra xóa files
    deleted = temp_storage.delete_file(path1)
    assert deleted is True
    assert temp_storage.exists(path1) is False

    # Danh sách file giảm xuống còn 1
    files_after = temp_storage.list_files("bronze/test_list", pattern="*.parquet")
    assert len(files_after) == 1


# =====================================================================
# 2. KIỂM THỬ DEAD LETTER QUEUE (DLQ)
# =====================================================================


def test_dlq_write_error_from_dataframe(
    dlq_handler: DeadLetterQueue, temp_storage: PipelineStorage
):
    """Kiểm tra ghi nhận lỗi từ một DataFrame chứa các bản ghi hỏng."""
    df_corrupted = pd.DataFrame(
        {"review_id": ["ERR_01"], "star_rating": [99]}  # Rating vi phạm rule (1-5)
    )

    output_path = dlq_handler.write_error(
        data=df_corrupted,
        layer="bronze",
        source_file="amazon_reviews_test.tsv",
        error_message="Invalid star_rating value: 99",
    )

    assert output_path.exists()
    assert "dlq/bronze" in str(output_path)

    # Đọc lại file JSON lỗi để kiểm tra cấu trúc
    errors = dlq_handler.read_errors("bronze")
    assert len(errors) == 1

    err_data = errors[0]
    assert err_data["layer"] == "bronze"
    assert err_data["source_file"] == "amazon_reviews_test.tsv"
    assert err_data["error_message"] == "Invalid star_rating value: 99"
    assert err_data["record_count"] == 1
    assert err_data["records"][0]["star_rating"] == 99


def test_dlq_write_error_from_string_or_dict(dlq_handler: DeadLetterQueue):
    """Kiểm tra ghi nhận lỗi dạng chuỗi raw message hoặc dict."""
    output_path = dlq_handler.write_error(
        data="Raw text line corrupted...",
        layer="silver",
        source_file="bad_chunk.parquet",
        error_message="Parse Exception",
    )

    assert output_path.exists()

    errors = dlq_handler.read_errors("silver")
    assert len(errors) == 1
    assert errors[0]["records"][0]["raw_data"] == "Raw text line corrupted..."
