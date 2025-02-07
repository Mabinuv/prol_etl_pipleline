from airflow import DAG # Import DAG for defining workflows
from airflow.providers.http.hooks.http import HttpHook # HTTP Hook for API requests
from airflow.providers.postgres.hooks.postgres import PostgresHook  # PostgreSQL Hook for DB interactions
from airflow.decorators import task  # Task decorator to define Airflow tasks
from airflow.utils.dates import days_ago  # Utility to set DAG start date
import requests  # Used internally by Airflow HttpHook
import json  # For handling JSON responses

# Define coordinates for the location
latitude = '57.708870'
longitude = '11.974560'

# Define Airflow Connection IDs
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = 'open_meteo_api'

# Default arguments for DAG execution
default_args = {
    'owner' : 'airflow',
    'start_date': days_ago(1)
}

## Define the DAG
with DAG(dag_id = 'weather_etl_pipeline',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dag:
    
    @task()
    def extract_weather_data():
        """
        Extract task: Fetches weather data from Open-Meteo API
        """
        ### this will get the connection details from airflow
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET') 

        ## Build api endpoint
        endpoint = f'/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true'

        ## Make the request via http hook
        response = http_hook.run(endpoint)

        # Check response status
        if response.status_code == 200:
            return response.json()
        else: 
            raise Exception(f'Failed to fetch weather data: {response.status_code}')
        
    @task()
    def transform_weather_data(weather_data):
        """
        Transform task: Extracts relevant weather details from the API response
        """
        current_weather = weather_data['current_weather'] # Extract 'current_weather' field from API response
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
        """
        Load task: Inserts the transformed data into a PostgreSQL database
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Alter table if needed to update new field to existing table 
        cursor.execute("""
        ALTER TABLE weather_data
        ADD COLUMN IF NOT EXISTS is_day INT
        """)

        # Create table if it does not exist
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

         # Insert transformed weather data into the table
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

        ## Commit transaction adn close cursor
        conn.commit()
        cursor.close()

#### Define DAG workflow: Extract → Transform → Load      
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)


