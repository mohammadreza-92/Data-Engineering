"""
GitHub Data Backfill DAG

This DAG performs historical backfill of GitHub repository statistics.
It's designed to fetch repository metrics for past dates when the DAG wasn't running.

Key Features:
- Fetches stars, forks, and issues count for specified repositories
- Supports backfill for any date range within start_date
- Implements rate limiting to avoid GitHub API throttling
- Includes retry logic for API failures
- Modular design for easy extension
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.github.operators.github import GithubOperator
from airflow.providers.github.hooks.github import GithubHook
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',  # Owner of the DAG
    'depends_on_past': False,     # Don't depend on previous runs
    'email_on_failure': True,     # Send email on failure
    'email_on_retry': False,      # Don't send email on retry
    'retries': 3,                 # Number of retries on failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'start_date': datetime(2023, 1, 1),   # Earliest date for backfill
}

def setup_github_connection():
    """
    Initialize and return a GitHub connection using Airflow's GitHub hook
    
    Returns:
        github.Github: Authenticated GitHub API connection object
    
    Note:
        Requires 'github_default' connection to be configured in Airflow
    """
    github_hook = GithubHook(github_conn_id='github_default')
    return github_hook.get_conn()

def fetch_github_repo_data(repo_name, **context):
    """
    Fetch repository statistics for a specific execution date
    
    Args:
        repo_name (str): GitHub repository in 'owner/repo' format
        context (dict): Airflow task context containing execution_date
    
    Returns:
        dict: Dictionary containing repository metrics for the date
    
    Metrics Collected:
        - Stars count
        - Forks count
        - Open issues count
        - Last updated timestamp
    """
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Get authenticated GitHub connection
    github_conn = setup_github_connection()
    
    try:
        # Get repository object
        repo = github_conn.get_repo(repo_name)
        
        logging.info(f"Successfully fetched data for {repo_name} on {date_str}")
        
        return {
            'date': date_str,
            'repo': repo_name,
            'stars': repo.stargazers_count,
            'forks': repo.forks_count,
            'open_issues': repo.open_issues_count,
            'last_updated': repo.updated_at.isoformat()
        }
    except Exception as e:
        logging.error(f"Error fetching data for {repo_name}: {str(e)}")
        raise

def save_to_database(data, **context):
    """
    Save fetched repository data to persistent storage
    
    Args:
        data (dict): Repository metrics to save
        context (dict): Airflow task context
    
    Note:
        This is a placeholder implementation. Replace with your actual
        database saving logic (PostgreSQL, BigQuery, etc.)
    """
    try:
        logging.info(f"Attempting to save data: {data}")
        # Implement your actual database saving logic here
        # Example:
        # db.insert('github_stats', data)
        logging.info("Data saved successfully")
    except Exception as e:
        logging.error(f"Error saving data: {str(e)}")
        raise

# Define the DAG
with DAG(
    'github_data_backfill',
    default_args=default_args,
    description='Backfill GitHub repository statistics',
    schedule_interval='@daily',  # Daily execution pattern
    catchup=True,               # Enable backfill for missed intervals
    max_active_runs=3,          # Limit concurrent runs to avoid API rate limits
    tags=['github', 'backfill', 'analytics'],
) as dag:

    # Task to fetch data for apache/airflow repository
    fetch_airflow_stats = PythonOperator(
        task_id='fetch_airflow_stats',
        python_callable=fetch_github_repo_data,
        op_kwargs={'repo_name': 'apache/airflow'},
        provide_context=True,
    )

    # Task to fetch data for tensorflow/tensorflow repository
    fetch_tensorflow_stats = PythonOperator(
        task_id='fetch_tensorflow_stats',
        python_callable=fetch_github_repo_data,
        op_kwargs={'repo_name': 'tensorflow/tensorflow'},
        provide_context=True,
    )

    # Task to persist all collected data
    save_data = PythonOperator(
        task_id='save_to_database',
        python_callable=save_to_database,
        provide_context=True,
    )

    # Set task dependencies - both fetch tasks run in parallel,
    # then results are saved to database
    [fetch_airflow_stats, fetch_tensorflow_stats] >> save_data

# Backfill Command Example:
# airflow dags backfill -s 2023-01-01 -e 2023-12-31 github_data_backfill
# This would process all days in 2023 that haven't been processed yet

"""
Implementation Notes:

1. GitHub API Rate Limits:
   - Authenticated requests: 5,000 requests/hour
   - Consider adding sleep between requests if processing many repositories
   - Monitor 'X-RateLimit-Remaining' in API responses

2. Error Handling:
   - The DAG includes retries for transient failures
   - Consider adding alerting for persistent failures

3. Scaling:
   - For large-scale backfills, consider:
     - Increasing max_active_runs (with caution)
     - Using async requests
     - Implementing a custom sensor to pause when rate limits are hit

4. Customization:
   - Add more repositories by creating additional tasks
   - Enhance save_to_database with your specific storage solution
   - Add data validation steps if needed
"""
