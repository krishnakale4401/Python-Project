import os
from zipfile import ZipFile

print(os.getcwd())
import json
import pandas as pd
from utils import parse_file_date_from_name

from pathlib import Path
from datetime import datetime, date
Data_Ingestion_dir = Path(__file__).resolve().parents[1]/'data'/'ingestion'
Data_Parquet_dir = Path(__file__).resolve().parents[1]/'data'/'Preprocessed'

def preprocess_file(validated_files):
    for i in validated_files:
        print('==========================================================================================================')
        df = pd.read_csv(Data_Ingestion_dir/i)
        df = df.drop_duplicates()

        df['File_Date'] = parse_file_date_from_name(i)
        df['Ingestion_Date'] = datetime.now()
        print(df)

        result = i.split('.')
        print(result[0])
        parquet_file = Data_Parquet_dir/f'{result[0]}.parquet'
        print(parquet_file)
        df.to_parquet(parquet_file)

    return True, 'Success'