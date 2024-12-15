from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.dummy import DummyOperator
import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_temp_graph(**kwargs):
    file_name = kwargs['dag_run'].conf.get('file_name')
    input_path = f"/opt/airflow/input/Sudno_a/{file_name}"  # Путь к файлу
    output_dir = f"/opt/airflow/output/{file_name}"
    os.makedirs(output_dir, exist_ok=True)

    # Читаем CSV и строим график
    df = pd.read_csv(input_path)
    plt.plot(df['Depth (m)'], df['Temperature (Celsius)'], label='Temperature')
    plt.xlabel('Depth (m)')
    plt.ylabel('Temperature (Celsius)')
    plt.title('Temperature vs Depth')
    plt.savefig(os.path.join(output_dir, 'temperature_depth.png'))
    # В конце функции добавьте:
    kwargs['ti'].xcom_push(key='processing_status', value='success')

def dummy_task(**kwargs):
    import logging
    logging.info("End task executed for DAG: %s", kwargs['dag'].dag_id)

with DAG(
    'temp_graph',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule_interval=None,
    catchup=False,
) as dag:
    plot_temp_task = PythonOperator(
        task_id='plot_temp_graph',
        python_callable=plot_temp_graph,
        provide_context=True,  # Это устарело, но для старых версий можно оставить
    )

    # Используем DummyOperator для завершения DAG
    end_task = DummyOperator(task_id='end_task')

    plot_temp_task >> end_task  # Задача plot_temp_task будет выполнена перед end_task