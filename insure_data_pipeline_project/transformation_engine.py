import glob
from pathlib import Path
import pandas as pd

# Access file path process

Data_transformation_Dir =Path(__file__).resolve().parents[1]/'data'/'curated'
Control_dir =Path(__file__).resolve().parents[1]/'data'/'preprocessed'
Data_Sementic_Dir = Path(__file__).resolve().parents[1]/'data'/'semantic'

# check file path and extension pattern

def load_processed(file_pattern):
    files = list(Path(Control_dir).glob(file_pattern))
    print('files',files)
    if not files:
        raise FileNotFoundError(f'files not found:[{file_pattern}]')
    dfs = []   # create blank list
    for i in files:
        res = pd.read_parquet(i)
        dfs.append(res)
    df = pd.concat(dfs, ignore_index=True)
    return df

def build_curated_and_semantic():
    Control_file = sorted(glob.glob(str(Control_dir)+ '/*.parquet'))
    print('path',Control_file) # file path


    claims = load_processed('claims_*.parquet')
    customers = load_processed('customers_*.parquet')
    policies = load_processed('policies_*.parquet')
    payments = load_processed('payments_*.parquet')

# file row and column count
    print('claims', claims.shape)
    print('customers', customers.shape)
    print('policies', policies.shape)
    print('payments', payments.shape)

    #merge data

    curated = (
        claims
        .merge(policies, on= 'Policy_ID', how='left', suffixes=('_claims', '_policy'))
        .merge(customers, on= 'Customer_ID', how='left')
        .merge(payments, on= 'Policy_ID', how='left', suffixes=('', '_payment'))
    )
    # make file parquet and stored in curated folder
    path = Data_transformation_Dir / 'claims_enriched.parquet'
    curated.to_parquet(path, index=False)


    claims_enriched = pd.read_parquet(path)
    pd.set_option('display.max_column', None)
    pd.set_option('display.max_row',None)
    pd.set_option('display.width',None)

    print('claims_enriched',claims_enriched.shape)
    print('claims_enriched',claims_enriched.head())

    print('krishna')

    df1 = pd.read_parquet('D:\insure_data_pipeline_project\dev_insure_data_pipeline\data\curated\claims_enriched.parquet')
    print(df1.shape) # file row column count
    # first kpi
    df = df1.groupby('Policy_Type').agg({'Claim_Amount':'sum', 'Claim_ID':'count'})
    df.columns=['Total_claim_amount', 'Total_claim_Id']
    print(df)

    df.to_parquet(Data_Sementic_Dir/'Policy_Wise.parquet')
    df.to_csv(Data_Sementic_Dir/'Policy_Wise.csv')

    # second kpi
    df = df1.groupby('City').agg({'Claim_Amount':'sum', 'Claim_ID':'count'})
    df.to_parquet(Data_Sementic_Dir/'City_Wise.parquet')
    df.to_csv(Data_Sementic_Dir/'City_Wise.csv')
    print(df)

    return None