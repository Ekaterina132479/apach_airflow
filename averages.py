from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator
import pandas as pd
import os

def calculate_averages(**kwargs):
    file_name = kwargs['dag_run'].conf.get('file_name')
    input_path = f"/opt/airflow/input/Sudno_a/{file_name}"  # Путь к файлу
    output_dir = f"/opt/airflow/output/{file_name}"
    os.makedirs(output_dir, exist_ok=True)

    # Читаем CSV и вычисляем средние значения
    df = pd.read_csv(input_path)
    averages = df.mean(numeric_only=True)
    averages.to_csv(os.path.join(output_dir, 'averages.csv'))
    kwargs['ti'].xcom_push(key='processing_status', value='success')


with DAG(
    'calculate_averages',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule_interval=None,
    catchup=False,
) as dag:
    calculate_averages_task = PythonOperator(
        task_id='calculate_averages',
        python_callable=calculate_averages,
        provide_context=True,
    )

end_task = DummyOperator(task_id='end_task')
calculate_averages_task >> end_task