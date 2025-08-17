from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime

def hello_world():
    print("Pipeline de combustíveis rodando")

if __name__ == "__main__":
    hello_world()
