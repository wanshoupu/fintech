import asyncio
import datetime
import os.path

import pandas as pd

from investment.utils.finviz_formatter import clean_finviz
from investment.utils.finviz_scraper import scrape_to_file
from investment.utils.finviz_views import FinVizView

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook

GBUCKET = 'us-central1-linviz-scraping-bb899a79-bucket'

FINVIZ_RAW = 'finviz-raw'
FINVIZ_CLEAN = 'finviz-clean'


def upload_to_gcs(bucket_name, object_name, local_file):
    hook = GCSHook()
    hook.upload(bucket_name=bucket_name, object_name=object_name, filename=local_file)


def asyncio_scrape():
    date = datetime.datetime.today()
    filename = f'{date.year}-{date.month}-{date.day}'
    scraped_file = asyncio.run(scrape_to_file(FinVizView.ALL, start_offset=1, base_filename=filename))
    upload_to_gcs(GBUCKET, os.path.join(FINVIZ_RAW, filename), scraped_file)


def data_cleaning():
    date = datetime.datetime.today()
    filename = f'{date.year}-{date.month}-{date.day}.parquet'
    input_file = os.path.join(FINVIZ_RAW, filename)
    df = pd.read_parquet(input_file)

    typed_df = clean_finviz(df)
    useful_cols = typed_df.select_dtypes(exclude=['object'])
    typed_df.to_parquet(filename, index=False)

    output_file = os.path.join(FINVIZ_RAW, filename)
    upload_to_gcs(GBUCKET, os.path.join(FINVIZ_CLEAN, output_file), filename)


with DAG(
        dag_id='finviz_download_daily_job',
        description='Run Finviz scraping job daily',
        schedule='@daily',  # <- Run daily
        start_date=datetime(2025, 1, 1),
        catchup=False,  # Don't backfill runs before today
        tags={'etl', 'scrape'},
) as dag:
    scrape_task = PythonOperator(
        task_id='asyncio_scrape',
        python_callable=asyncio_scrape,
    )
    data_cleaning_task = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning,
    )
    scrape_task >> data_cleaning_task
