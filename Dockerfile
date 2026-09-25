FROM python:3.11-slim

WORKDIR /app

# Cài đặt các hệ thống cần thiết
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements và cài đặt python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pytest

# Copy toàn bộ source code
COPY . .

# Mặc định chạy main pipeline ở chế độ full load
CMD ["python", "main_pipeline.py", "--mode", "full", "--n", "100000"]
