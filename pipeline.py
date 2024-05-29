import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import sqlite3

URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/{TYPE}_{YEAR}-{MONTH}.parquet"

#data ingestion
def fetch_data(TYPE, YEAR, MONTH):
    url = URL_TEMPLATE.format(TYPE=TYPE, YEAR=YEAR, MONTH=MONTH)
    response = requests.get(url)
    if response.status_code == 200:
        return pd.read_parquet(BytesIO(response.content))
    else:
        print(f"Data for {TYPE}-{YEAR}-{MONTH} NOT AVAILABLE")
        return None

#preprocessing the data
def process_data(df, TYPE):
    df = df.dropna() #drop NaN value, alternatively fillna
    if TYPE == 'yellow_tripdata':
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
        df['pickup_day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
        df['dropoff_hour'] = df['tpep_dropoff_datetime'].dt.hour
        df['dropoff_day_of_week'] = df['tpep_dropoff_datetime'].dt.dayofweek
    elif TYPE == 'green_tripdata':
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        df['pickup_hour'] = df['lpep_pickup_datetime'].dt.hour
        df['pickup_day_of_week'] = df['lpep_pickup_datetime'].dt.dayofweek
        df['dropoff_hour'] = df['lpep_dropoff_datetime'].dt.hour
        df['dropoff_day_of_week'] = df['lpep_dropoff_datetime'].dt.dayofweek
    return df

def save_parquet(df, table_name):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{table_name}.parquet")
    print(f"Data for {table_name} stored in Parquet format successfully.")

def save_avro(df, table_name):
    for col in df.columns:
        df[col]=df[col].astype(str)
    records = df.to_dict('records')
    schema = {
        "type": "record",
        "name": table_name,
        "fields": [{"name": col, "type": ["null", "string","int"]} for col in df.columns]
    }
    with open(f"{table_name}.avro", "wb") as out:
        fastavro.writer(out, schema, records)
    print(f"Data for {table_name} stored in Avro format successfully.")

def store_data_in_db(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Data for {table_name} stored in database successfully.")

def month_data_exists(conn, table_name, year, month):
    if table_name == 'yellow_tripdata':
        query = f"SELECT 1 FROM {table_name} WHERE strftime('%Y', tpep_pickup_datetime) = ? AND strftime('%m', tpep_pickup_datetime) = ? LIMIT 1;"
    else:
        query = f"SELECT 1 FROM {table_name} WHERE strftime('%Y', lpep_pickup_datetime) = ? AND strftime('%m', lpep_pickup_datetime) = ? LIMIT 1;"    
    cursor = conn.execute(query, (year, month))
    return cursor.fetchone() is not None

def fetch_and_process_data_for_last_three_years(TYPE, conn):

    current_date = datetime.now()
    start_date = current_date - timedelta(days=3*365)

    for year in range(start_date.year, current_date.year + 1):
        for month in range(1, 13):

            if year == current_date.year and month > current_date.month:
                break
            month_str = f"{month:02d}"  # Ensure month is two digits

            table_name = f"{TYPE}"
            t2= f"{TYPE}_{year}_{month_str}"

             # Check if data for this month and year is already in the table
            if month_data_exists(conn, TYPE, year, month_str):
                print(f"Data for {TYPE} {year}-{month_str} already exists in the database. Skipping...")
                continue           

            df = fetch_data(TYPE, year, month_str)

            if df is not None:
                df_processed = process_data(df, TYPE)
                store_data_in_db(df_processed, TYPE, conn)
                save_parquet(df_processed, t2)
                save_avro(df_processed, t2)

def main():
    TYPES = ["yellow_tripdata", "green_tripdata"]

    conn = sqlite3.connect('tripdata.db')

    for type_ in TYPES:
        fetch_and_process_data_for_last_three_years(type_, conn)

    conn.close()

if __name__ == "__main__":
    main()
