import pandas as pd

def read_bioportal(path):
    df = pd.read_excel(path)
    df['collectedDate'] = pd.to_datetime(df['collectedDate'], format='%m/%d/%Y')
    df['reportedDate'] = pd.to_datetime(df['reportedDate'], format='%m/%d/%Y', errors='coerce')
    df['createdAt'] = pd.to_datetime(df['createdAt'], format='%m/%d/%Y %H:%M')
    return df

def write_bioportal(df, path):
    df['collectedDate'] = df['collectedDate'].dt.strftime('%Y-%m-%d')
    df['reportedDate'] = df['reportedDate'].dt.strftime('%Y-%m-%d')
    df.to_csv(path, index_label='id')