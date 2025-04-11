# Databricks notebook source
pip install apache-airflow-providers-apache-spark


# COMMAND ----------

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# COMMAND ----------

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

# Default arguments for DAG tasks
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='spark_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for user event aggregation using Spark',
    schedule_interval='@daily',  # Runs every day at midnight
    catchup=False,               # Prevents backfilling of past runs
    tags=['spark', 'etl', 'user-events'],
) as dag:

    # Task 0: Start point for better readability
    start_task = DummyOperator(
        task_id='start'
    )

    # Task 1: Wait for data availability in DBFS or local path
    data_sensor = FileSensor(
        task_id='data_availability_sensor',
        filepath="/FileStore/tables/pyspark_txt",  # Ensure this path exists in DBFS
        poke_interval=60,  # Check every 60 seconds
        timeout=600,       # Timeout after 10 minutes
        mode='poke',
    )

    # Task 2: Run the Spark job
    spark_job = SparkSubmitOperator(
        task_id='run_spark_job',
        application="/FileStore/tables/spark_script.py",  # Spark script path
        conn_id='spark_default',  # Must be defined in Airflow Connections
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
        },
        verbose=True
    )

    # Task 3: Dummy end task
    end_task = DummyOperator(
        task_id='end'
    )

    # Define task pipeline flow
    start_task >> data_sensor >> spark_job >> end_task
