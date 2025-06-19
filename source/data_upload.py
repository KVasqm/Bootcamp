import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
import os

# Configuración de rutas y conexión
CLEAN_DATA_PATH = "./files_dump/clean_data/"
VEH_TYPE = "yellow_tripdata_"
TABLE_NAME = "taxi_trips"

# Conexión a PostgreSQL
db_string = "postgresql+psycopg2://postgres:Pingo33@localhost:5432/tlc_ny"
CONN = create_engine(db_string)

def data_range(start_: str, end_: str) -> list:
    """
    Devuelve una lista de strings en formato 'YYYY-MM' entre start_ y end_.
    """
    start_date = pd.to_datetime(start_, format="%Y%m")
    end_date = pd.to_datetime(end_, format="%Y%m") + pd.DateOffset(months=1)
    date_range = pd.date_range(start_date, end_date, freq="ME")
    return date_range.strftime("%Y-%m").tolist()

def upload_month_chunks(date_: str, chunk_size_=10000) -> None:
    """
    Carga en la base de datos los datos de un mes en chunks.

    Args:
        date_ (str): Fecha en formato 'YYYY-MM'.
        chunk_size_ (int): Tamaño de los bloques a cargar.
    """
    file_name = f"{VEH_TYPE}{date_}.parquet"
    file_path = os.path.join(CLEAN_DATA_PATH, file_name)

    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f" Cargando archivo: {file_name}")

    df = pd.read_parquet(file_path)
    total_rows = df.shape[0]

    for i in tqdm(range(0, total_rows, chunk_size_), desc=f"{date_} chunks"):
        df_chunk = df[i:i+chunk_size_]
        df_chunk.to_sql(
            name=TABLE_NAME,
            con=CONN,
            schema="yellow",
            if_exists="append",
            index=False,
            method='multi'
        )

if __name__ == "__main__":
    # Definir rango de fechas deseado
    DATA_RANGE = data_range("202409", "202412")

    for date in tqdm(DATA_RANGE, desc="Meses cargados"):
        upload_month_chunks(date)

    print(" Proceso finalizado.")
