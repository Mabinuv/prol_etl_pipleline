from airflow import DAG 
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json


latitude = '57.708870'
longitude = '11.974560'

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner' : 'airflow',
    'start_date': days_ago(1)
}

## DAG
with DAG(dag_id = 'weather_etl_pipeline',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dag:
    
    @task()
    def extract_weather_data():
        ### this will get the connection details from airflow
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET') 

        ## Build api endpoint
        endpoint = f'/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true'

        ## Make the request via http hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else: 
            raise Exception(f'Failed to fetch weather data: {response.status_code}')
        
    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude' : latitude,
            'longitude' : longitude,
            'temperature' : current_weather['temperature'],
            'windspeed' : current_weather['windspeed'],
            'winddirection' : current_weather['winddirection'],
            'is_day': current_weather['is_day'],
            'weathercode' : current_weather['weathercode']
        }    
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        ALTER TABLE weather_data
        ADD COLUMN IF NOT EXISTS is_day INT
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            is_day INT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)


        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, is_day, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s, %s )               
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['is_day'],
            transformed_data['weathercode'],
        ))

        conn.commit()
        cursor.close()

### DAG work flow        
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)


