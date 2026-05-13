#configs/schemas/review_schemas.py

REQUIRED_COLUMNS = [
    'review_id',
    'customer_id',      # Thêm để định danh khách hàng
    'product_id',
    'product_category',  # Thêm để phân loại ngành hàng
    'review_body',
    'review_headline',   # Thêm để lấy tiêu đề review
    'star_rating',
    'verified_purchase', # Thêm để kiểm tra độ xác thực
    'review_date',
]