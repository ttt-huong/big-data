import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
import pandas as pd


@pytest.fixture
def sample_raw_data():
    """Tạo một DataFrame chứa cả dữ liệu hợp lệ và nhiều loại dữ liệu lỗi."""
    return pd.DataFrame([
        # 1. Hợp lệ
        {
            "image_id": 1,
            "filename": "img_1.jpg",
            "file_size": 102400,
            "width": 1920,
            "height": 1080,
            "format": "jpg",
            "category": "san_pham",
            "created_at": "2024-05-10 10:00:00"
        },
        # 2. Hợp lệ
        {
            "image_id": 2,
            "filename": "img_2.png",
            "file_size": 204800,
            "width": 1080,
            "height": 1080,
            "format": "png",
            "category": "phong_canh",
            "created_at": "2024-06-15 14:30:00"
        },
        # 3. Lỗi: NULL_IMAGE_ID
        {
            "image_id": None,
            "filename": "img_3.jpg",
            "file_size": 50000,
            "width": 800,
            "height": 600,
            "format": "jpg",
            "category": "do_an",
            "created_at": "2024-01-01 00:00:00"
        },
        # 4. Lỗi: INVALID_FILE_SIZE (0)
        {
            "image_id": 4,
            "filename": "img_4.webp",
            "file_size": 0,
            "width": 1280,
            "height": 720,
            "format": "webp",
            "category": "chan_dung",
            "created_at": "2024-03-20 08:15:00"
        },
        # 5. Lỗi: INVALID_CATEGORY & INVALID_FORMAT
        {
            "image_id": 5,
            "filename": "img_5.gif",
            "file_size": 30000,
            "width": 500,
            "height": 500,
            "format": "gif",
            "category": "category_khong_ton_tai",
            "created_at": "2024-02-10 12:00:00"
        },
        # 6. Lỗi: DUPLICATE_IMAGE_ID (id = 1 đã có ở trên)
        {
            "image_id": 1,
            "filename": "img_1_dup.jpg",
            "file_size": 102400,
            "width": 1920,
            "height": 1080,
            "format": "jpg",
            "category": "san_pham",
            "created_at": "2024-05-10 10:00:00"
        },
        # 7. Lỗi: FUTURE_CREATED_AT (ngày trong tương lai)
        {
            "image_id": 7,
            "filename": "img_7.jpg",
            "file_size": 99999,
            "width": 1920,
            "height": 1080,
            "format": "jpg",
            "category": "dong_vat",
            "created_at": "2099-01-01 00:00:00"
        }
    ])
