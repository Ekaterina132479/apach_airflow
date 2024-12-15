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
    plt.plot(df['Depth (m)'], df['Salinity (PSU)'], label='Salinity')
    plt.xlabel('Depth (m)')
    plt.ylabel('Salinity (PSU)')
    plt.title('Salinity vs Depth')
    plt.savefig(os.path.join(output_dir, 'salinity_depth.png'))
    kwargs['ti'].xcom_push(key='processing_status', value='success')


with DAG(
    'salinity_graph',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule_interval=None,
    catchup=False,
) as dag:
    plot_salinity_task = PythonOperator(
        task_id='plot_salinity_graph',
        python_callable=plot_temp_graph,
        provide_context=True,
    )

end_task = DummyOperator(task_id='end_task')
plot_salinity_task >> end_task