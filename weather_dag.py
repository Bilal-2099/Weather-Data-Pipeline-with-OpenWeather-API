from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import pandas as pd
import os

# Function to save weather data to a CSV file
def save_weather_data_to_csv(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

    # Check if the data is available
    if data is None:
        raise ValueError("No data retrieved from the API.")

    # Prepare the directory to save the CSV
    os.makedirs('data', exist_ok=True)

    # Create a DataFrame from the API response
    transformed_data = {
        "City": data["name"],
        "Description": data["weather"][0]['description'],
        "Temperature (K)": data["main"]["temp"],
        "Feels Like (K)": data["main"]["feels_like"],
        "Min Temperature (K)": data["main"]["temp_min"],
        "Max Temperature (K)": data["main"]["temp_max"],
        "Pressure": data["main"]["pressure"],
        "Humidity": data["main"]["humidity"],
        "Wind Speed": data["wind"]["speed"],
        "Time of Record": datetime.utcfromtimestamp(data['dt']),
        "Sunrise": datetime.utcfromtimestamp(data['sys']['sunrise']),
        "Sunset": datetime.utcfromtimestamp(data['sys']['sunset']),
    }
    
    # Create DataFrame and save it as a CSV
    df_data = pd.DataFrame([transformed_data])  # Ensure it is a list of dicts
    now = datetime.now()
    dt_string = now.strftime("%Y%m%d%H%M%S")
    filename = f"data/current_weather_data_{dt_string}.csv"
    df_data.to_csv(filename, index=False)

    return filename  # Return the filename for the next task

# Function to upload the CSV file to S3
def upload_csv_to_s3(local_file):
    bucket_name = 'S3_Bucket_Name'  # Replace with your actual S3 bucket name
    object_name = os.path.basename(local_file)  # Just the filename, no folder structure

    s3_hook = S3Hook(aws_conn_id='aws_default')  # Make sure to configure your Airflow connection for AWS
    s3_hook.load_file(filename=local_file, key=object_name, bucket_name=bucket_name, replace=True)

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 2),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Create the DAG
with DAG('weather_data_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&appid=API_Key',  # Replace with your actual API key
    )

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&appid=API_Key',  # Replace with your actual API key
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    save_weather_data = PythonOperator(
        task_id='save_weather_data',
        python_callable=save_weather_data_to_csv
    )

    upload_task = PythonOperator(
        task_id='upload_csv_to_s3',
        python_callable=upload_csv_to_s3,
        op_kwargs={'local_file': "{{ task_instance.xcom_pull(task_ids='save_weather_data') }}"}  # Pull the filename from the previous task
    )

    # Set task dependencies
    is_weather_api_ready >> extract_weather_data >> save_weather_data >> upload_task
