#!/usr/bin/env python
# coding: utf-8




import pandas as pd
from sqlalchemy import create_engine

# File parameters
year = 2021
month = 1
chunksize = 100000
# DB parameters
pg_user="root"
pg_pass="root"
pg_host="localhost"
pg_port=5432
pg_db="ny_taxi"
target_table = "yellow_taxi_data"



dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(
    prefix + 'yellow_tripdata_{year}-{month:02d}.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)

# nrows=100,
# Display first rows
df.head()

# Check data types
#df.dtypes

# Check data shape
#df.shape






engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
conn = engine.connect() 





print(pd.io.sql.get_schema(df, name=target_table, con=engine))





df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')





df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_{year}-{month:02d}.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=chunksize
)

first = True
for df_chunk in df_iter:

    if first:
        # Create table schema (no data)
        df_chunk.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace"
        )
        first = False
        print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))




dff = pd.read_sql(f"SELECT * FROM {target_table}", engine)
dff.head()