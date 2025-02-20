# dags/quality_check_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from utils.metric_collectors import (
    connect_to_db, collect_completeness_metrics,
    collect_uniqueness_metrics, save_metrics_to_db
)
from utils.notifications import send_slack_notification, send_email_alert

# Load environment variables
load_dotenv()

# Database connection strings
SOURCE_DB_CONN = f"postgresql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASSWORD')}@localhost:5432/source_database"
QUALITY_DB_CONN = f"postgresql://{os.getenv('QUALITY_DB_USER')}:{os.getenv('QUALITY_DB_PASSWORD')}@localhost:5432/quality_metrics"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 18),
    'email_on_failure': True,
    'email': 'data_team@example.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_checks',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

def run_completeness_check(**kwargs):
    source_engine = connect_to_db(SOURCE_DB_CONN)
    quality_engine = connect_to_db(QUALITY_DB_CONN)
    
    # Example tables to check
    tables_to_check = [
        {'table': 'customers', 'columns': ['customer_id', 'email', 'phone']},
        {'table': 'orders', 'columns': ['order_id', 'customer_id', 'order_date']}
    ]
    
    for table_config in tables_to_check:
        table = table_config['table']
        for column in table_config['columns']:
            metrics = collect_completeness_metrics(source_engine, table, column)
            save_metrics_to_db(metrics, quality_engine)
            
            # Alert if completeness is below threshold
            if metrics['completeness_pct'] < 95:
                message = f"ALERT: {table}.{column} completeness is {metrics['completeness_pct']:.2f}% (below 95% threshold)"
                send_slack_notification(message)
                send_email_alert("Data Quality Alert", message)

def run_uniqueness_check(**kwargs):
    source_engine = connect_to_db(SOURCE_DB_CONN)
    quality_engine = connect_to_db(QUALITY_DB_CONN)
    
    # Columns that should be unique
    unique_checks = [
        {'table': 'customers', 'column': 'customer_id'},
        {'table': 'orders', 'column': 'order_id'}
    ]
    
    for check in unique_checks:
        metrics = collect_uniqueness_metrics(source_engine, check['table'], check['column'])
        save_metrics_to_db(metrics, quality_engine)
        
        # Alert if uniqueness is not 100% for primary keys
        if metrics['uniqueness_pct'] < 100:
            message = f"CRITICAL: {check['table']}.{check['column']} uniqueness is {metrics['uniqueness_pct']:.2f}% (should be 100%)"
            send_slack_notification(message)
            send_email_alert("CRITICAL Data Quality Alert", message)

completeness_task = PythonOperator(
    task_id='check_data_completeness',
    python_callable=run_completeness_check,
    provide_context=True,
    dag=dag
)

uniqueness_task = PythonOperator(
    task_id='check_data_uniqueness',
    python_callable=run_uniqueness_check,
    provide_context=True,
    dag=dag
)

completeness_task >> uniqueness_task