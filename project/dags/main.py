from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
import os
import json
import time
import logging
from requests.exceptions import ConnectionError, HTTPError
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

TMP_BTC_DATA_PATH = "/tmp/btc_data/btc_data.csv"
TMP_TRUMP_DATA_PATH = "/tmp/trump_data/trump_data.csv"

default_args = {
    'owner': 'Guerreiro - Izuel',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}


def load_btc_data(**kwargs):
    btc_df = pd.read_csv('../data/btc_15m_data_2018_to_2025.csv')
    column_mapping = {col: col.lower().replace(' ', '_') for col in btc_df.columns}
    btc_df.rename(columns=column_mapping, inplace=True)

    btc_df['close_time'] = pd.to_datetime(btc_df['close_time'])

    # Extract only close and close_time columns
    btc_df = btc_df[['close_time', 'close']].copy()

    # Filter for entries at market closing time (15:59:59.999000 or 16:00:00)
    # We'll look for times that are close to 16:00 (15:59:59.999000)
    market_close_time = pd.Timestamp('15:59:59.999000').time()

    # Filter for the closing time entries
    btc_df = btc_df[btc_df['close_time'].dt.time == market_close_time].copy()
    btc_df.to_csv(TMP_BTC_DATA_PATH, index=False)

def load_trump_data(**kwargs):
  trump_df = pd.read_csv('../data/trump_tweets_2024.csv')
  column_mapping = {col: col.lower().replace(' ', '_') for col in trump_df.columns}
  trump_df.rename(columns=column_mapping, inplace=True)
  trump_df['created_at'] = pd.to_datetime(trump_df['created_at'])
  trump_df.to_csv(TMP_TRUMP_DATA_PATH, index=False)


def clean_btc_data(**kwargs):
  pass

def clean_trump_data(**kwargs):
  pass

def filter_data(**kwargs):
  pass

def moderate_text(**kwargs):
  pass

def generate_dataset(**kwargs):
  pass

def save_dataset(**kwargs):
  pass


# DAG
with DAG(
    dag_id='pokemon_base_etl_parallel',
    description='DAG ETL paralelo que une data de /pokemon y /pokemon-species',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['pokemon', 'parallel', 'etl']
) as dag:

    fetch_pokemon_list = HttpOperator(
        task_id='fetch_pokemon_list',
        http_conn_id='pokeapi',
        endpoint=f'/pokemon?limit={POKEMON_LIMIT}',
        method='GET',
        log_response=True,
        response_filter=lambda response: response.text,
        do_xcom_push=True,
    )

    download_a = PythonOperator(
        task_id='download_pokemon_data',
        python_callable=download_pokemon_data,
    )

    download_b =  PythonOperator(
        task_id='download_species_data',
        python_callable=download_species_data,
    )

    merge_transform = PythonOperator(
        task_id='merge_and_transform_data',
        python_callable=merge_and_transform_data,
    )

    fetch_pokemon_list >> [download_a, download_b] >> merge_transform