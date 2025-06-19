"""
Steps to clean data:
1) Create column date only
2) Order Cols
3) clean date: integrate two previous functions
4) save clean data: sate data to a parquet file
"""

import pandas as pd
from tqdm import tqdm

RAW_DATA_PATH = "./files_dump/raw_data/"
CLEAN_DATA_PATH = "./files_dump/clean_data/"
VEH_TYPE = "yellow_tripdata_"

def order_cols(df_: pd.DataFrame) -> pd.DataFrame:
    """
    Move the last column to the 4th position.

    Args:
        df_ (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    # cols in lower case
    cols = df_.columns.tolist()
    cols = cols[:2] + cols[-1:] + cols[2:-1]
    df_ = df_[cols]
    df_.columns = df_.columns.str.lower()
    return df_

def clean_data(df_: pd.DataFrame) -> pd.DataFrame:
    """
    Clean data by creating a new column with only the date part of the 'tpep_pickup_datetime' column
    and moving the last column to the 4th position.

    Args:
        df_ (pd.DataFrame): DataFrame to modify.

    Returns:
        pd.DataFrame: modified DataFrame.
    """
    df_["tpep_pickup_datetime"] = df_["tpep_pickup_datetime"].dt.date
    df_ = order_cols(df_)
    for col in df_.columns:
        if df_[col].dtype == "int32":
            df_[col] = df_[col].astype("int64")
    return df_

def data_range(start_: str, end_: str) -> list:
    # Convertir a datatime start_
    start_date = pd.to_datetime(start_, format="%Y%m")
    # convertir a datatime end_
    end_date = pd.to_datetime(end_, format="%Y%m") + pd.DateOffset(months=1)
    # rango de fechas entre start_date and end_date
    date_range = pd.date_range(start_date, end_date, freq="ME")
    # convertir string y YYYY-MM
    date_range_list = date_range.strftime("%Y-%m").tolist()
    print(date_range_list)
    return date_range_list

def save_clean_data(data_range_: list) -> None:
    """
    Save a DataFrame to a parquet file.

    Args:
        df_ (pd.DataFrame): The DataFrame to save.
        file_name (str): The name of the file to save the DataFrame to.
    """
    for date in tqdm(data_range_):
        file_name = f"{VEH_TYPE}{date}.parquet"
        df = pd.read_parquet(f"{RAW_DATA_PATH}{file_name}")
        df = clean_data(df)
        df.to_parquet(f"{CLEAN_DATA_PATH}{file_name}", index=False)

if __name__ == "__main__":
    start_date = "202407"
    end_date = "202412"
    date_range_list = data_range(start_date, end_date)
    save_clean_data(date_range_list)