"raw_data_download.py step by step"
import pandas as pd
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
# year-month.parquet | 2021-01.parquet
DW_PATH = "./files_dump/raw_data/yellow_tripdata_"
#type hinting

def get_data_month(date_: str) -> None:
    # creo la  URL
    url = f"{BASE_URL}{date_}.parquet"
    print(url)  
    # descargo el archivo
    df = pd.read_parquet(url)
    #guardo el archivo
    df.to_parquet(
        f"{DW_PATH}{date_}.parquet",
        index=False
    )
    print(f"Archivo {DW_PATH}{date_}.parquet descargado correctamente")

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

def get_data_range(start_date: str, end_date: str) -> None:
    data_range_ = data_range(start_date, end_date)
    for date in data_range_:
        get_data_month(date)

if __name__ == "__main__":
    start_date = "202304"
    end_date = "202412"
    get_data_range(start_date,end_date)