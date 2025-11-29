import os

print(os.getcwd())
import json
import pandas as pd

from pathlib import Path
from audit_logger import log_Ingestion
from utils import parse_file_date_from_name #,now_iso

Data_Ingestion_dir = Path(__file__).resolve().parents[1]/'data'/'ingestion'
Control_dir=Path(__file__).resolve().parents[1]/'config'/'control_files'
print('Data_Ingestion_dir', Data_Ingestion_dir)
def validate_file(cf):
    #print('in validate function')
    control=json.loads(open(cf).read())
    fname = control['file_name']
    #print(fname)
    file_date = parse_file_date_from_name(fname)
    # print(file_date)

    expected_ex = control.get('expected_extension')
    expected_rows = control.get('expected_rows')
    expected_columns = control.get('expected_columns',[])
    #print(expected_ex)
    #print(expected_rows)
    #print(expected_columns)
    file_path = Data_Ingestion_dir/fname
    #print(file_path)



# file available or not check validation
    if not file_path.exists():
        log_Ingestion(fname, file_date, expected_rows, 0,'failed', 'file_missing')
        return False,"File does not exist"

# file name and extension check file name should be lower and extension will be .csv
    if not fname.lower().endswith(expected_ex.lower()):
        log_Ingestion(fname, file_date, expected_rows,0,'failed', 'file_Extension_missing')
        return False, "file does not have the expected extension"

 # row count and column count
    try:
        df=pd.read_csv(file_path)
    except Exception as e:
        log_Ingestion(fname,file_date,expected_rows,0,'failed',f'read_error:{e}')
        return False, f'Read_error:{e}'

    print('=========================================================================================================')
    actual_rows = len(df)
    actual_columns =list(df.columns)
    print(actual_rows)
    print(expected_rows)
    print(actual_columns)
    print(expected_columns)

    if expected_rows is not None and actual_rows != expected_rows:
        log_Ingestion(fname,file_date,expected_rows,actual_rows,'failed', 'Row_Count_Mismatch')
        file_name, file_date, expected_rows, actual_rows, expected_columns, expected_ex, status, error_message = None
        return False, 'Row_Count_Mismatch'

    if expected_columns is not None and actual_columns != expected_columns:
        log_Ingestion(fname, file_date, expected_rows, actual_rows, expected_columns, 'Column_Mismatch')
        return False, 'Column Mismatch'


    log_Ingestion(fname, file_date, expected_rows, actual_rows, expected_columns, 'success')
    return True, 'Success'