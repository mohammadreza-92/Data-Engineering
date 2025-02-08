from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import pandas as pd
import psycopg2
import requests
import logging
import smtplib

# DAG Configuration with default arguments
default_args = {
    'owner': 'airflow',  # Defines the owner of the DAG
    'depends_on_past': False,  # DAG does not depend on past runs
    'start_date': days_ago(1),  # Defines when the DAG starts
    'retries': 3,  # Number of retries on task failure
}

# Function to extract data from an API
def extract_data_from_api():
    url = "https://api.example.com/data"  # API endpoint URL
    response = requests.get(url)  # Make GET request to API
    if response.status_code == 200:
        return response.json()  # Return JSON data if request is successful
    else:
        raise Exception("Failed to fetch data from API")  # Raise an exception if request fails

# Function to transform data using Pandas
def transform_data(**kwargs):
    ti = kwargs['ti']  # Task instance to pull data from XCom
    raw_data = ti.xcom_pull(task_ids='extract')  # Pull extracted data from XCom
    df = pd.DataFrame(raw_data)  # Convert raw data into a Pandas DataFrame
    df['processed_at'] = pd.Timestamp.now()  # Add a timestamp column
    df.dropna(inplace=True)  # Remove rows with missing values
    df['value'] = df['value'] * 1.2  # Example transformation: Increase 'value' by 20%
    transformed_data = df.to_dict(orient='records')  # Convert DataFrame back to dictionary
    ti.xcom_push(key='transformed_data', value=transformed_data)  # Push transformed data to XCom

# Function to load transformed data into PostgreSQL
def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']  # Task instance to pull transformed data from XCom
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')  # Get transformed data from XCom
    conn = psycopg2.connect(
        dbname="destination_db", user="user", password="password", host="localhost"
    )  # Establish connection to PostgreSQL database
    cursor = conn.cursor()  # Create a cursor object to execute SQL commands
    for record in data:
        cursor.execute(
            "INSERT INTO target_table (id, name, value, processed_at) VALUES (%s, %s, %s, %s)",
            (record['id'], record['name'], record['value'], record['processed_at'])
        )  # Insert each record into the target table
    conn.commit()  # Commit transaction to save changes
    cursor.close()  # Close cursor
    conn.close()  # Close database connection

# Function to send email notification upon DAG completion
def send_notification():
    try:
        server = smtplib.SMTP('smtp.example.com', 587)  # Connect to SMTP server
        server.starttls()  # Start TLS encryption
        server.login('user@example.com', 'password')  # Login to email server
        message = "Subject: DAG Execution\n\nETL DAG executed successfully."
        server.sendmail('user@example.com', 'recipient@example.com', message)  # Send email notification
        server.quit()  # Close SMTP connection
    except Exception as e:
        logging.error("Failed to send email: %s", str(e))  # Log error if email fails

# Define the DAG with schedule interval and task dependencies
with DAG('complex_etl_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    # Define ETL task group
    with TaskGroup("etl_group") as etl_group:
        
        # Task to extract data
        extract = PythonOperator(
            task_id='extract',
            python_callable=extract_data_from_api  # Calls function to extract data
        )
        
        # Task to transform extracted data
        transform = PythonOperator(
            task_id='transform',
            python_callable=transform_data,
            provide_context=True  # Provides execution context to the function
        )
        
        # Task to load transformed data into PostgreSQL
        load = PythonOperator(
            task_id='load',
            python_callable=load_data_to_postgres,
            provide_context=True  # Provides execution context to the function
        )
        
        # Define task dependencies within ETL group
        extract >> transform >> load
    
    # Task to send notification upon successful DAG execution
    notify = PythonOperator(
        task_id='notify',
        python_callable=send_notification
    )
    
    # Set task dependencies: ETL tasks must finish before notification task runs
    etl_group >> notify
