from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import os
import re
import logging
from google.cloud import language_v2

# Configurar logging
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

# Rutas de archivos temporales
TMP_BTC_DATA_PATH = "/tmp/btc_data/btc_processed.csv"
TMP_TRUMP_DATA_PATH = "/tmp/trump_data/trump_processed.csv"
TMP_MERGED_DATA_PATH = "/tmp/merged_data/final_dataset.csv"

# Configuración de Google Cloud API
LANGUAGE_CODE = "en"
# API_KEY se obtiene de variables de entorno o Airflow Variables

default_args = {
    'owner': 'Guerreiro - Izuel',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}


def load_btc_data(**kwargs):
    """Carga y procesa los datos de BTC"""
    logging.info("Cargando datos de BTC...")
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(TMP_BTC_DATA_PATH), exist_ok=True)
    
    # Buscar archivos de datos en múltiples ubicaciones posibles
    possible_paths = [
        '/opt/airflow/dags/../data/btc_15m_data_2018_to_2025.csv',
        '/opt/airflow/data/btc_15m_data_2018_to_2025.csv',
        '/usr/local/airflow/data/btc_15m_data_2018_to_2025.csv',
        '/data/btc_15m_data_2018_to_2025.csv',
        './data/btc_15m_data_2018_to_2025.csv',
        '../data/btc_15m_data_2018_to_2025.csv'
    ]
    
    btc_file_path = None
    for path in possible_paths:
        if os.path.exists(path):
            btc_file_path = path
            logging.info(f"Archivo BTC encontrado en: {path}")
            break
    
    if btc_file_path is None:
        raise FileNotFoundError(f"No se pudo encontrar el archivo btc_15m_data_2018_to_2025.csv en ninguna de las ubicaciones: {possible_paths}")
    
    # Cargar datos de BTC
    btc_df = pd.read_csv(btc_file_path)
    logging.info(f"Cargados {len(btc_df)} registros de BTC")
    
    # Renombrar columnas a snake_case
    column_mapping = {col: col.lower().replace(' ', '_') for col in btc_df.columns}
    btc_df.rename(columns=column_mapping, inplace=True)
    
    # Convertir close_time a datetime
    btc_df['close_time'] = pd.to_datetime(btc_df['close_time'])
    
    # Extraer solo columnas necesarias
    btc_df = btc_df[['close_time', 'close']].copy()
    
    # Filtrar por horario de cierre de mercado (15:59:59.999000)
    market_close_time = pd.Timestamp('15:59:59.999000').time()
    btc_df = btc_df[btc_df['close_time'].dt.time == market_close_time].copy()
    
    logging.info(f"Datos filtrados: {len(btc_df)} registros de cierre diario")
    
    # Guardar datos procesados
    btc_df.to_csv(TMP_BTC_DATA_PATH, index=False)
    logging.info(f"Datos de BTC guardados en {TMP_BTC_DATA_PATH}")

def load_trump_data(**kwargs):
    """Carga y procesa los datos de tweets de Trump"""
    logging.info("Cargando datos de tweets de Trump...")
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(TMP_TRUMP_DATA_PATH), exist_ok=True)
    
    # Buscar archivos de datos en múltiples ubicaciones posibles
    possible_paths = [
        '/opt/airflow/dags/../data/trump_tweets.csv',
        '/opt/airflow/data/trump_tweets.csv',
        '/usr/local/airflow/data/trump_tweets.csv',
        '/data/trump_tweets.csv',
        './data/trump_tweets.csv',
        '../data/trump_tweets.csv'
    ]
    
    trump_file_path = None
    for path in possible_paths:
        if os.path.exists(path):
            trump_file_path = path
            logging.info(f"Archivo Trump encontrado en: {path}")
            break
    
    if trump_file_path is None:
        raise FileNotFoundError(f"No se pudo encontrar el archivo trump_tweets.csv en ninguna de las ubicaciones: {possible_paths}")
    
    # Cargar datos de tweets
    trump_df = pd.read_csv(trump_file_path)
    logging.info(f"Cargados {len(trump_df)} tweets")
    
    # Seleccionar columnas relevantes
    trump_df = trump_df[['date', 'text', 'favorites', 'retweets']].copy()
    
    # Convertir fecha al formato correcto
    trump_df['date'] = pd.to_datetime(trump_df['date'], format='%m/%d/%Y %H:%M')
    
    logging.info(f"Datos de Trump procesados: {len(trump_df)} tweets")
    
    # Guardar datos procesados
    trump_df.to_csv(TMP_TRUMP_DATA_PATH, index=False)
    logging.info(f"Datos de Trump guardados en {TMP_TRUMP_DATA_PATH}")


def filter_and_clean_data(**kwargs):
    """Filtra y limpia los datos de ambos datasets"""
    logging.info("Iniciando filtrado y limpieza de datos...")
    
    # Cargar datos procesados
    btc_df = pd.read_csv(TMP_BTC_DATA_PATH)
    trump_df = pd.read_csv(TMP_TRUMP_DATA_PATH)
    
    # Convertir fechas
    btc_df['close_time'] = pd.to_datetime(btc_df['close_time'])
    trump_df['date'] = pd.to_datetime(trump_df['date'])
    
    # Filtrar tweets después del primer registro de BTC
    btc_start_date = btc_df['close_time'].min()
    trump_df = trump_df[trump_df['date'] >= btc_start_date].copy()
    logging.info(f"Tweets filtrados por fecha: {len(trump_df)} tweets")
    
    # Filtrar tweets que son principalmente URLs
    def is_mostly_url(text):
        """Verifica si el texto es principalmente URLs (más del 80%)"""
        if not text or pd.isna(text):
            return True
        
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, text)
        
        if not urls:
            return False
        
        total_url_length = sum(len(url) for url in urls)
        return total_url_length / len(text) > 0.8
    
    # Aplicar filtro de URLs
    initial_count = len(trump_df)
    trump_df = trump_df[~trump_df['text'].apply(is_mostly_url)].copy()
    logging.info(f"Tweets filtrados por URLs: {initial_count} -> {len(trump_df)} tweets")
    
    # Filtrar BTC hasta una semana después del último tweet
    trump_last_date = trump_df['date'].max()
    trump_first_date = trump_df['date'].min()
    btc_df = btc_df[btc_df['close_time'] <= trump_last_date + pd.Timedelta(days=7)].copy()
    btc_df = btc_df[btc_df['close_time'] >= trump_first_date - pd.Timedelta(days=7)].copy()
    logging.info(f"Datos BTC filtrados: {len(btc_df)} registros")
    
    # Guardar datos filtrados
    btc_df.to_csv(TMP_BTC_DATA_PATH, index=False)
    trump_df.to_csv(TMP_TRUMP_DATA_PATH, index=False)
    
    logging.info("Filtrado y limpieza completados")

def moderate_text(**kwargs):
    """Aplica moderación de texto usando Google Cloud Language API"""
    logging.info("Iniciando moderación de texto...")
    
    # Cargar datos de tweets
    trump_df = pd.read_csv(TMP_TRUMP_DATA_PATH)
    trump_df['date'] = pd.to_datetime(trump_df['date'])
    
    # Configurar cliente de Google Cloud Language
    # Nota: Para pruebas, limitamos a 10 tweets para evitar costos altos de API
    if len(trump_df) > 10:
        logging.warning(f"Limitando análisis a 10 tweets para evitar costos de API (total: {len(trump_df)})")
        trump_df = trump_df.head(10).copy()
    
    def moderate_single_text(text_content):
        """Modera un texto individual usando Google Cloud Language API"""
        try:
            # Obtener API key de variables de Airflow o variables de entorno
            api_key = None
            try:
                # Intentar obtener de Airflow Variables primero
                api_key = Variable.get("GOOGLE_CLOUD_API_KEY", default_var=None)
            except:
                # Si falla, intentar obtener de variables de entorno
                api_key = os.getenv("GOOGLE_CLOUD_API_KEY")
            
            if not api_key:
                logging.warning("API_KEY no configurada, usando valores por defecto")
                # Retornar valores por defecto si no hay API key
                return {
                    'Toxic': 0.0, 'Insult': 0.0, 'Profanity': 0.0, 'Derogatory': 0.0,
                    'Sexual': 0.0, 'Death, Harm & Tragedy': 0.0, 'Violent': 0.0,
                    'Firearms & Weapons': 0.0, 'Public Safety': 0.0, 'Health': 0.0,
                    'Religion & Belief': 0.0, 'Illicit Drugs': 0.0, 'War & Conflict': 0.0,
                    'Politics': 0.0, 'Finance': 0.0, 'Legal': 0.0
                }
            
            client = language_v2.LanguageServiceClient(
                client_options={"api_key": api_key}
            )
            
            document = {
                "content": text_content,
                "type_": "PLAIN_TEXT",
                "language_code": LANGUAGE_CODE,
            }
            
            response = client.moderate_text(request={"document": document})
            return {cat.name: cat.confidence for cat in response.moderation_categories}
            
        except Exception as e:
            logging.error(f"Error en moderación de texto: {e}")
            # Retornar valores por defecto en caso de error
            return {
                'Toxic': 0.0, 'Insult': 0.0, 'Profanity': 0.0, 'Derogatory': 0.0,
                'Sexual': 0.0, 'Death, Harm & Tragedy': 0.0, 'Violent': 0.0,
                'Firearms & Weapons': 0.0, 'Public Safety': 0.0, 'Health': 0.0,
                'Religion & Belief': 0.0, 'Illicit Drugs': 0.0, 'War & Conflict': 0.0,
                'Politics': 0.0, 'Finance': 0.0, 'Legal': 0.0
            }
    
    # Obtener categorías de moderación del primer tweet
    sample_categories = moderate_single_text(trump_df['text'].iloc[0])
    category_names = list(sample_categories.keys())
    
    # Inicializar columnas para cada categoría
    for category in category_names:
        trump_df[category] = 0.0
    
    # Procesar cada tweet
    for idx, text in trump_df['text'].items():
        try:
            categories = moderate_single_text(text)
            for category, confidence in categories.items():
                trump_df.at[idx, category] = confidence
        except Exception as e:
            logging.error(f"Error procesando tweet en índice {idx}: {e}")
            continue
    
    logging.info(f"Moderación completada para {len(trump_df)} tweets")
    
    # Guardar datos con moderación
    trump_df.to_csv(TMP_TRUMP_DATA_PATH, index=False)
    logging.info("Datos con moderación guardados")

def add_btc_price_data(**kwargs):
    """Agrega precios de BTC 24h y 48h después de cada tweet"""
    logging.info("Agregando datos de precios BTC...")
    
    # Cargar datos
    trump_df = pd.read_csv(TMP_TRUMP_DATA_PATH)
    btc_df = pd.read_csv(TMP_BTC_DATA_PATH)
    
    # Convertir fechas
    trump_df['date'] = pd.to_datetime(trump_df['date'])
    btc_df['close_time'] = pd.to_datetime(btc_df['close_time'])
    
    # Inicializar columnas para precios BTC
    trump_df['btc_24h_after'] = None
    trump_df['btc_48h_after'] = None
    
    # Procesar cada tweet
    for idx, row in trump_df.iterrows():
        tweet_date = row['date']
        tweet_day = tweet_date.date()
        
        # Calcular fechas objetivo (1 y 2 días después del tweet)
        target_24h_day = tweet_day + pd.Timedelta(days=1)
        target_48h_day = tweet_day + pd.Timedelta(days=2)
        
        # Buscar precio BTC 24h después
        btc_24h_mask = btc_df['close_time'].dt.date == target_24h_day
        if btc_24h_mask.any():
            closest_24h_idx = btc_df[btc_24h_mask].index[0]
            trump_df.at[idx, 'btc_24h_after'] = btc_df.at[closest_24h_idx, 'close']
        
        # Buscar precio BTC 48h después
        btc_48h_mask = btc_df['close_time'].dt.date == target_48h_day
        if btc_48h_mask.any():
            closest_48h_idx = btc_df[btc_48h_mask].index[0]
            trump_df.at[idx, 'btc_48h_after'] = btc_df.at[closest_48h_idx, 'close']
    
    # Filtrar tweets que tienen datos de BTC disponibles
    btc_data_available = trump_df[
        (trump_df['btc_24h_after'].notna()) | (trump_df['btc_48h_after'].notna())
    ].copy()
    
    logging.info(f"Dataset final: {len(btc_data_available)} tweets con datos BTC disponibles")
    
    # Guardar dataset final
    os.makedirs(os.path.dirname(TMP_MERGED_DATA_PATH), exist_ok=True)
    btc_data_available.to_csv(TMP_MERGED_DATA_PATH, index=False)
    logging.info(f"Dataset final guardado en {TMP_MERGED_DATA_PATH}")

def save_final_dataset(**kwargs):
    """Guarda el dataset final en el directorio de datos"""
    logging.info("Guardando dataset final...")
    
    # Cargar dataset final
    final_df = pd.read_csv(TMP_MERGED_DATA_PATH)
    
    # Definir ruta de salida
    output_path = '/tmp/output/trump_btc_analysis_dataset.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Guardar dataset final
    final_df.to_csv(output_path, index=False)
    
    logging.info(f"Dataset final guardado exitosamente en {output_path}")
    logging.info(f"Dataset contiene {len(final_df)} registros con {final_df.shape[1]} columnas")
    
    # Mostrar estadísticas básicas
    logging.info("Estadísticas del dataset final:")
    logging.info(f"- Tweets totales: {len(final_df)}")
    logging.info(f"- Rango de fechas: {final_df['date'].min()} a {final_df['date'].max()}")
    logging.info(f"- Tweets con precio BTC 24h: {final_df['btc_24h_after'].notna().sum()}")
    logging.info(f"- Tweets con precio BTC 48h: {final_df['btc_48h_after'].notna().sum()}")


# DAG
with DAG(
    dag_id='trump_btc_analysis_etl',
    description='ETL pipeline para análisis de tweets de Trump y precios de Bitcoin',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['trump', 'bitcoin', 'sentiment', 'etl', 'analysis']
) as dag:

    # Tarea 1: Cargar datos de BTC
    load_btc_task = PythonOperator(
        task_id='load_btc_data',
        python_callable=load_btc_data,
    )

    # Tarea 2: Cargar datos de tweets de Trump
    load_trump_task = PythonOperator(
        task_id='load_trump_data',
        python_callable=load_trump_data,
    )

    # Tarea 3: Filtrar y limpiar datos (depende de ambas cargas)
    filter_clean_task = PythonOperator(
        task_id='filter_and_clean_data',
        python_callable=filter_and_clean_data,
    )

    # Tarea 4: Aplicar moderación de texto
    moderate_task = PythonOperator(
        task_id='moderate_text',
        python_callable=moderate_text,
    )

    # Tarea 5: Agregar precios BTC 24h/48h después
    add_btc_prices_task = PythonOperator(
        task_id='add_btc_price_data',
        python_callable=add_btc_price_data,
    )

    # Tarea 6: Guardar dataset final
    save_final_task = PythonOperator(
        task_id='save_final_dataset',
        python_callable=save_final_dataset,
    )

    # Definir dependencias del DAG
    # Cargar datos en paralelo
    [load_btc_task, load_trump_task] >> filter_clean_task
    
    # Procesar secuencialmente después del filtrado
    filter_clean_task >> moderate_task >> add_btc_prices_task >> save_final_task