# server_metrics_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 9),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'server_metrics_pipeline',
    default_args=default_args,
    description='Pipeline for server metrics data',
    schedule_interval='*/15 * * * *',  # Run every 15 minutes
    catchup=False,
)

# Task to transform bronze to silver layer
bronze_to_silver = BigQueryExecuteQueryOperator(
    task_id='bronze_to_silver_transformation',
    sql='silver_layer_transformation.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
    dag=dag,
)

# Tasks to transform silver to gold layer
silver_to_gold_hourly = BigQueryExecuteQueryOperator(
    task_id='silver_to_gold_hourly',
    sql='gold_layer_hourly_aggregates.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
    dag=dag,
)

silver_to_gold_health = BigQueryExecuteQueryOperator(
    task_id='silver_to_gold_health',
    sql='gold_layer_server_health.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
    dag=dag,
)

silver_to_gold_location = BigQueryExecuteQueryOperator(
    task_id='silver_to_gold_location',
    sql='gold_layer_location_performance.sql',
    use_legacy_sql=False,
    bigquery_conn_id='bigquery_default',
    dag=dag,
)

# Set task dependencies
bronze_to_silver >> [silver_to_gold_hourly, silver_to_gold_health, silver_to_gold_location]
