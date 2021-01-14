from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
import os
from dags.hello import hello

args = {
    "owner": "airflow",
    "start_date": datetime(2019, 3, 30),
}


def hello_world(**context):
    hook = SnowflakeHook(snowflake_conn_id="sf_analytics_db")
    hello()

    return True


# Create the DAG with the parameters and schedule
dag = DAG("hello_world", catchup=False, default_args=args, schedule_interval=None)


with dag:
    say_hello = PythonOperator(
        task_id="hello_world", python_callable=hello_world, provide_context=True
    )
    say_hello2 = PythonOperator(
        task_id="hello_world2", python_callable=hello_world, provide_context=True
    )

if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()