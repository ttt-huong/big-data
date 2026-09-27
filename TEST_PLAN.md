# Ke hoach kiem thu Local ETL/Lakehouse

## 1. Muc tieu

Xac nhan pipeline local chay dung va co the demo tren mot may Windows. Pham vi kiem thu gom:

- Extract tu CSV va Parquet theo chunk.
- Validation, DLQ va idempotency.
- Delta Lake local, Full Load, Incremental Load va Backfill.
- Retry khi ghi Delta gap loi tam thoi.
- Phat hien du lieu den muon.
- DuckDB analytics va benchmark local.

## 2. Pham vi va gioi han

Day la ung dung dong lenh, khong co frontend web. Bang chung he thong chay dung la ket qua Pytest, summary tren terminal, Delta table local, file DLQ, watermark va DuckDB analytics.

Khong nam trong pham vi:

- MinIO, Docker, Spark, Kafka, Airflow, Hadoop hay he thong phan tan.
- Chay dong thoi nhieu pipeline tren nhieu may.
- Bao dam hieu nang o quy mo vuot qua tai nguyen cua may local.

Full Load xoa Delta table va DLQ cu truoc khi rebuild. Khong chay Full Load neu can giu lai ket qua demo truoc do.

## 3. Moi truong

Yeu cau: Windows, Python 3.13 hoac phien ban tuong thich, dung luong dia va RAM du cho du lieu benchmark.

```powershell
git switch feature/rebuild-etl-lakehouse
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m pip check
```

## 4. Kiem thu tu dong

Chay truoc moi lan demo va truoc khi merge:

```powershell
python -m compileall .
python -m pytest tests -v
```

Tieu chi dat:

- Khong co loi cu phap.
- Toan bo test pass.
- Khong co test bi skip hoac xfail ma chua duoc giai thich.

Kiem thu chuc nang trong bo Pytest:

| Nhom | Muc dich | Bang chung |
| --- | --- | --- |
| Generator va validation | Sinh du lieu tai lap, tach record sai, bat duplicate | `test_generator_validation.py` |
| Extract va watermark | Doc chunk, Full/Incremental, cap nhat watermark | `test_pipeline.py` |
| Delta va analytics | MERGE idempotent, DuckDB doc clean/DLQ | `test_pipeline.py`, `test_analytics.py` |
| Backfill va DLQ | Khong doi watermark, rerun khong trung | `test_backfill.py` |
| Retry | Retry loi tam thoi, dung khi loi het so lan, khong retry loi logic | `test_retry.py` |
| Late-arriving data | Gan `is_late`, giu watermark tang dan, nap late record | `test_late_arriving.py` |

Chay rieng cac tinh huong quan trong khi can:

```powershell
python -m pytest tests/test_retry.py tests/test_late_arriving.py -v
```

## 5. Kiem thu tich hop end-to-end

Chay theo dung thu tu duoi day:

```powershell
python main_pipeline.py --mode full --rows 1000 --error-ratio 0.05 --chunk-size 250
python main_pipeline.py --mode incremental --rows 300 --error-ratio 0.02 --chunk-size 100
python main_pipeline.py --mode backfill --rows 1000 --backfill-before-id 500 --chunk-size 100
python query_analytics.py
```

Sau moi lenh pipeline, xac nhan:

- `status` la `SUCCESS`.
- `extracted_records = valid_records + error_records`.
- `loaded_records` bang so record clean o luot chay do.
- Khong co dong `[ERROR]` trong `logs/pipeline.log`.

Xac nhan artifact:

```powershell
Get-ChildItem data\lakehouse\clean_metadata
Get-ChildItem data\errors
Get-Content data\watermark.json
Get-Content logs\pipeline.log -Tail 50
```

Tieu chi dat:

- Delta table ton tai trong `data/lakehouse/clean_metadata`.
- Record loi duoc ghi thanh Parquet trong `data/errors` va co `error_reason`.
- Incremental khong nap lai source batch da xu ly.
- Backfill khong lam thay doi `last_image_id` trong watermark.
- DuckDB analytics doc duoc ca clean data va DLQ.

## 6. Kiem thu loi va bien

| Tinh huong | Cach kiem tra | Ket qua mong doi |
| --- | --- | --- |
| Rerun incremental | Chay lai cung source batch | Khong them record trung |
| Loi tam thoi khi load | Chay `test_retry.py` | Retry theo so lan cau hinh, du lieu van idempotent |
| Loi load lien tuc | Chay test retry exhausted | Pipeline fail, watermark khong duoc cap nhat |
| Record loi | Tang `--error-ratio` | Record vao DLQ, clean data van load |
| Record den muon | Chay `test_late_arriving.py` | `is_late = true`, record van duoc upsert |
| Tham so sai | `--rows -1`, `--error-ratio 1.5`, `--chunk-size 0` | Chuong trinh bao loi ro rang, khong lam hong state |

## 7. Kiem thu hieu nang va gioi han

Khong dung benchmark lon trong luot demo chinh. Chay sau khi da pass functional test:

```powershell
python -m benchmark.run_benchmarks
```

Do theo cac moc 10K, 50K, 100K; neu may du tai nguyen, tiep tuc 250K, 500K va 1M records. Ghi lai:

- Tong thoi gian va thoi gian Extract/Transform/Load.
- RAM cao nhat va dung luong dia.
- So chunk va chunk size.
- Dung luong Parquet, Delta Lake va DLQ.
- Thoi gian DuckDB analytics.

Dung tang quy mo khi may bat dau swap RAM, o dia gan day, thoi gian tang dot bien, hoac mot luot chay vuot thoi gian demo chap nhan duoc.

## 8. Tieu chi nghiem thu

Du an duoc xem la san sang demo khi:

- Toan bo Pytest pass.
- Full, Incremental, Backfill va analytics chay thanh cong tren may sach chi voi Python va `requirements.txt`.
- Delta MERGE va DLQ idempotent khi rerun.
- Watermark chi cap nhat sau khi load thanh cong; Backfill khong doi watermark.
- Retry va late-arriving data co test tu dong va ket qua co the truy vet.
- README, so do kien truc va output terminal mo ta dung hanh vi code.
