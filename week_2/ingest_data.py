#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"


@task(log_prints=True)
def download_data(url):

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O {csv_name}")

    return csv_name


@task(
    log_prints=True,
    retries=0,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(csv_name):

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    return df


@task(log_prints=True)
def transform_data(df):

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task
def load_data(df):
    
    table_name = "yellow_taxi_trips"

    database_block = SqlAlchemyConnector.load("postgres-connector")

    with database_block.get_connection(begin=False) as engine:

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

        df.to_sql(name=table_name, con=engine, if_exists="append")

    # while True:

    #     try:
    #         t_start = time()

    #         df = next(df_iter)

    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()

    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break


@flow(name="Ingest Flow")
def main():

    csv_name = download_data(url=URL)

    df = extract_data(csv_name)

    df_transformed = transform_data(df)

    load_data(df_transformed)


if __name__ == "__main__":
    main()
