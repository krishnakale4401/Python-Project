from datetime import date
from zipfile import ZipFile
from pathlib import Path
import os

from dev_insure_data_pipeline.src.ingestion_manager import validate_file

print(os.getcwd())

Data_Retention_Archive = Path(__file__).resolve().parents[1]/'data'/'retention'/'archive'
Data_Retention_file = Path(__file__).resolve().parents[1]/'data'/'ingestion'

def archive_file(validated_files):
    dt = date.today()
    zip_filename = Data_Retention_Archive / f'Ingestion_File_{dt}.zip'

    with ZipFile(zip_filename, 'a') as zipf:


        #print(validated_files)
        for i in validated_files:
            file_path = Data_Retention_file/i

            if file_path.exists():
                zipf.write(file_path, arcname = i)

            #os.remove(Data_Remove_Ingestion/i)
    return None

