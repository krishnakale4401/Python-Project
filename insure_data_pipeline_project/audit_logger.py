import datetime
from pathlib import Path
import csv



base_dir = Path(__file__).resolve().parents[1]
log_dir = base_dir / "logs"
log_dir.mkdir(exist_ok=True)
Ingestion_Log_file = log_dir / "Ingestion_Log.csv"

def write_csv(file_path, header,  row):
    file_exists = file_path.exists()

    with open(file_path, mode = 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
            writer.writerow(row)




def log_Ingestion(file_name,file_date,expected_rows,actual_rows,expected_columns,expected_ex,error_message=None):

    headers=['file_name','file_date','expected_rows','actual_rows','expected_columns','expected_ex','errer_message','ingestion_time']
    row=[
        file_name,
        file_date,
        expected_rows,
        actual_rows,
        expected_columns,
        expected_ex,
        error_message or '',
        datetime.datetime.now().isoformat()
    ]
    write_csv(Ingestion_Log_file,headers,row)

