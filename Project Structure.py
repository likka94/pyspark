# Databricks notebook source
project structure/
├── dags/
│   └── spark_etl_pipeline.py          ← Airflow DAG
│
├── scripts/
│   └── spark_etl_job.py              ← Spark ETL job
│
├── data/
│   ├── input/
│   │   ├── data.parquet
│   │   └── _SUCCESS                   ← Tells Airflow "data is ready"
│   └── output/                        ← Spark writes result here
