import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG


def extract():
    df = pd.read_csv("data/sales.csv")
    print("Data Extracted")
    return df


def transform(df):
    df = df.drop_duplicates()
    df = df.dropna()
    df["date"] = pd.to_datetime(df["date"])
    df["amount_with_tax"] = df["amount"] * 1.18
    print("Data Transformed")
    return df


def load(df):
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    engine = create_engine(connection_string)

    df.to_sql("sales", engine, if_exists="append", index=False)

    print("Data Loaded")


def run_etl():
    df = extract()
    df = transform(df)
    load(df)


if __name__ == "__main__":
    run_etl()