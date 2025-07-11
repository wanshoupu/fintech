import asyncio
import datetime
import os.path

import pandas as pd

from investment.utils.data_quality import bluechips
from investment.utils.finviz_formatter import clean_finviz
from investment.utils.finviz_scraper import scrape_to_file
from investment.utils.finviz_views import FinVizView

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook

GBUCKET = 'us-central1-linviz-scraping-bb899a79-bucket'

FINVIZ_RAW = 'data/finviz-raw'
FINVIZ_CLEAN = 'data/finviz-clean'
FINVIZ_BLUECHIPS = 'data/finviz-bluechips'
ESTIMATED_LENGTH = 10_500


def upload_to_gcs(bucket_name, object_name, local_file):
    hook = GCSHook()
    hook.upload(bucket_name=bucket_name, object_name=object_name, filename=local_file)


def data_scrape(offset, length, filename, **kwargs):
    scraped_file = scrape_to_file(FinVizView.ALL, offset=offset, length=length, filename=filename)
    ds = kwargs['ds']
    output_file = os.path.join(FINVIZ_RAW, ds, filename)
    upload_to_gcs(GBUCKET, output_file, scraped_file)


def data_cleaning(filenames, **kwargs):
    ds = kwargs['ds']
    inputs = [os.path.join(f'gs://{GBUCKET}', FINVIZ_RAW, ds, f) for f in filenames]
    df = pd.concat([pd.read_parquet(fp) for fp in inputs], ignore_index=True)

    df = clean_finviz(df)
    filename = f'{ds}.parquet'
    df.to_parquet(filename, index=False)

    output_file = os.path.join(FINVIZ_CLEAN, ds, filename)
    upload_to_gcs(GBUCKET, output_file, filename)


def data_bluechips(**kwargs):
    ds = kwargs['ds']
    filename = f'{ds}.parquet'
    input_file = os.path.join(f'gs://{GBUCKET}', FINVIZ_CLEAN, ds, filename)

    df = pd.read_parquet(input_file)
    df = bluechips(df)
    df.to_parquet(filename, index=False)

    output_file = os.path.join(FINVIZ_BLUECHIPS, ds, filename)
    upload_to_gcs(GBUCKET, output_file, filename)


with DAG(
        dag_id='finviz_download_daily_job',
        description='Run Finviz scraping job daily',
        schedule='@daily',  # <- Run daily
        start_date=datetime(2025, 1, 1),
        catchup=False,  # Don't backfill runs before today
        tags=['etl', 'scrape'],
) as dag:
    data_scrape_task = lambda offset, length, filename: PythonOperator(
        task_id=f'data_scrape_{offset}_{length}',
        python_callable=data_scrape,
        op_args=[offset, length, filename],
    )
    offsets = [offset for offset in range(1, ESTIMATED_LENGTH, 1000)]
    lengths = [1000] * len(offsets)
    filenames = [f'{offset}-{length}.parquet' for offset, length in zip(offsets, lengths)]
    scrape_tasks = [data_scrape_task(o, l, f) for o, l, f in zip(offsets, lengths, filenames)]

    data_cleaning_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning,
        op_args=[filenames],
    )

    data_bluechips_task = PythonOperator(
        task_id='data_bluechips',
        python_callable=data_bluechips,
    )
    scrape_tasks >> data_cleaning_task >> data_bluechips_task
