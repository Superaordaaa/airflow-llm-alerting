"""Example DAG: one task that always fails, so you can see the alert end to end."""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from alerting import on_failure, on_success_check_slow


def load_orders():
    rows = [{"order_id": 1, "amount": "12.5"}, {"order_id": 2, "amount": None}]
    return sum(float(r["amount"]) for r in rows)  # fails on None -> TypeError


with DAG(
    dag_id="example_llm_alerting",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "on_failure_callback": on_failure,
        "on_success_callback": on_success_check_slow,
        "retries": 0,
    },
    tags=["demo"],
) as dag:
    PythonOperator(task_id="load_orders", python_callable=load_orders)
