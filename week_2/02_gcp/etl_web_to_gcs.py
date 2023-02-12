from pathlib import Path

import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
@task()
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)

    return df

@task(log_prints=True)
def clean(df:pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    print(df.head(2))
    print("columns: {}".format(df.dtypes))
    print("rows: {}".format(len(df)))

    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:

    path = Path(f"./{dataset_file}.parquet")

    df.to_parquet(path, compression='gzip')

    return path

@task(log_prints=True)
def write_gsc(path: Path) ->None:

    gcp_block = GcsBucket.load("gcs-bucket")

    gcp_block.upload_from_path(
        from_path = path,
        to_path=path
    )



@flow()
def etl_web_to_gcs() -> None:
    """
    The main ETL function
    """

    colour = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{colour}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)

    df = clean(df)

    path = write_local(df, dataset_file)

    write_gsc(path)


if __name__ == "__main__":

    etl_web_to_gcs()
