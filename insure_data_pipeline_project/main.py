from pathlib import Path
from ingestion_manager import validate_file
from preprocessing_engine import preprocess_file
from retention_manager import archive_file
from transformation_engine import build_curated_and_semantic

import json
import glob

Control_dir=Path(__file__).resolve().parents[1]/'config'/'control_files'
print(Control_dir)

def run_pipeline():
    control_file=sorted(glob.glob(str(Control_dir) + '/*.json'))
    print(control_file)

    validated_files=[]

    for cf in control_file:
        print('=========================================================================================================')
        print(cf)
        ok,reason=validate_file(cf)
        if ok:
            with open(cf,'r') as f:
                ctrl=json.load(f)
            fname = ctrl['file_name']
            print(f'validate_file:{fname}')
            validated_files.append(fname)

        else:
            print(f' file validation error:{reason}')
    print('validated_files:', validated_files)
    print('===========================================================================================================================================')
    print('File Validation Successfully Completed')

    preprocess_file(validated_files)
    print('===========================================================================================================================================')
    print('Preprocess Layer Successfully Done')
    print('===========================================================================================================================================')


    build_curated_and_semantic()
    print('===========================================================================================================================================')
    print('Transformation Layer Successfully Done')
    print('===========================================================================================================================================')

    archive_file(validated_files)

if __name__ =='__main__':
    run_pipeline()