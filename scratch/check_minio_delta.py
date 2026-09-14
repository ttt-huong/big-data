import boto3, json, os, sys
import pathlib
# Ensure project root (big-data) is in sys.path
project_root = pathlib.Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
import config
import config
from deltalake import DeltaTable

def main():
    s3 = boto3.client(
        's3',
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
    )
    # list buckets
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    print('Buckets:', buckets)
    # check metadata bucket
    bucket = config.BUCKET_METADATA
    prefix = 'clean_metadata_delta/'
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [obj['Key'] for obj in resp.get('Contents', [])]
    print('Objects under clean_metadata_delta (first 20):', keys[:20])
    delta_log_exists = any(k.startswith(prefix + '_delta_log/') for k in keys)
    print('_delta_log exists:', delta_log_exists)
    # count data files (parquet)
    data_files = [k for k in keys if k.endswith('.parquet')]
    print('Number of parquet data files:', len(data_files))
    # load DeltaTable to get schema and file count
    try:
        dt = DeltaTable(f's3://{bucket}/{prefix.rstrip('/')}', storage_options=config.DELTA_STORAGE_OPTIONS)
        print('Delta table version:', dt.version())
        schema = dt.schema().to_pyarrow()
        print('Delta schema fields:', [f.name for f in schema])
        print('Delta table files count via history:', len(dt.history()))
    except Exception as e:
        print('Failed to load DeltaTable:', e)

if __name__ == '__main__':
    main()
