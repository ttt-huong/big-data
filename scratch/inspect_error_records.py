import pathlib, sys
# Ensure project root (big-data) is in sys.path
project_root = pathlib.Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
import config
import pyarrow.parquet as pq
import pandas as pd

# Ensure project root is in path
project_root = pathlib.Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

error_path = config.ERROR_RECORDS_PATH
print('Error records path:', error_path)

table = pq.read_table(error_path)
df = table.to_pandas()
print('Total error records:', len(df))
if 'error_reason' in df.columns:
    print('Error reason counts:')
    print(df['error_reason'].value_counts())
else:
    print('No error_reason column')
print('Schema fields:', [f.name for f in table.schema])
