from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import os
import time

# Пути
input_path = "/opt/airflow/input/Sudno_b"
processed_txt = "/opt/airflow/input/processed.txt"
processed_folder = "/opt/airflow/input/processed"
corrupted_folder = "/opt/airflow/input/corrupted"

def scan_and_trigger(**kwargs):
    # Проверка уже обработанных файлов
    processed_files = set()
    if os.path.exists(processed_txt):
        with open(processed_txt, "r") as file:
            processed_files = set(file.read().splitlines())

    # Сканируем папку input
    for file_name in os.listdir(input_path):
        file_path = os.path.join(input_path, file_name)
        if file_name in processed_files or not file_name.endswith(".csv"):
            continue

        try:
            # Имитируем задержку
            time.sleep(5)  # Костыль для предотвращения бесконечного выполнения

            # Проверяем корректность файла (например, структура CSV)
            with open(file_path, "r") as f:
                headers = f.readline().strip()
                if "Datetime,Depth (m)" not in headers:
                    raise ValueError("Invalid file structure")

            # Триггерим обработчики
            kwargs['ti'].xcom_push(key='file_name', value=file_name)
            return file_name  # Передаём имя файла для обработки

        except Exception as e:
            # Перемещаем файл в corrupted
            os.makedirs(corrupted_folder, exist_ok=True)
            os.rename(file_path, os.path.join(corrupted_folder, file_name))

    # Если не найден ни один файл для обработки
    return None

def move_to_processed(**kwargs):
    file_name = kwargs['ti'].xcom_pull(task_ids='scan_and_trigger')
    if not file_name:  # Если file_name = None, просто выходим из функции
        return

    # Проверяем статус обработки
    statuses = [
        kwargs['ti'].xcom_pull(dag_id='temp_graph', key='processing_status'),
        kwargs['ti'].xcom_pull(dag_id='salinity_graph', key='processing_status'),
        kwargs['ti'].xcom_pull(dag_id='calculate_averages', key='processing_status'),
    ]
    if not all(status == 'success' for status in statuses):
        raise ValueError("Processing not completed successfully for all DAGs")

    os.makedirs(processed_folder, exist_ok=True)
    os.rename(os.path.join(input_path, file_name), os.path.join(processed_folder, file_name))
    # Добавляем в processed.txt
    with open(processed_txt, "a") as file:
        file.write(f"{file_name}\n")


with DAG(
    'sudno_b_monitor',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule_interval=None,
    catchup=False,
) as dag:
    scan_and_trigger_task = PythonOperator(
        task_id='scan_and_trigger',
        python_callable=scan_and_trigger,
        provide_context=True,
    )

    # Условие на случай, если scan_and_trigger возвращает None
    def check_file_name(**kwargs):
        file_name = kwargs['ti'].xcom_pull(task_ids='scan_and_trigger')
        time.sleep(5)  # Костыль на случай повторного вызова
        return "skip_processing" if not file_name else "trigger_temp_dag"

    check_file_name_task = PythonOperator(
        task_id='check_file_name',
        python_callable=check_file_name,
        provide_context=True,
    )

    skip_processing = DummyOperator(task_id='skip_processing')

    trigger_temp_dag = TriggerDagRunOperator(
        task_id='trigger_temp_dag',
        trigger_dag_id='temp_graph',
        conf={"file_name": "{{ ti.xcom_pull(task_ids='scan_and_trigger') }}"},
    )

    trigger_salinity_dag = TriggerDagRunOperator(
        task_id='trigger_salinity_dag',
        trigger_dag_id='salinity_graph',
        conf={"file_name": "{{ ti.xcom_pull(task_ids='scan_and_trigger') }}"},
    )

    trigger_avg_dag = TriggerDagRunOperator(
        task_id='trigger_avg_dag',
        trigger_dag_id='calculate_averages',
        conf={"file_name": "{{ ti.xcom_pull(task_ids='scan_and_trigger') }}"},
    )

    wait_for_temp_dag = ExternalTaskSensor(
        task_id='wait_for_temp_dag',
        external_dag_id='temp_graph',
        external_task_id='end_task',  # Ожидает завершение всего DAG
        mode='reschedule',  # Оптимизация ожидания
        timeout=3600,  # Время ожидания, например, 1 час
        poke_interval=30,  # Интервал проверки
    )

    wait_for_salinity_dag = ExternalTaskSensor(
        task_id='wait_for_salinity_dag',
        external_dag_id='salinity_graph',
        external_task_id='end_task',  # Ожидает завершение всего DAG
        mode='reschedule',
        timeout=3600,
        poke_interval=30,
    )

    wait_for_avg_dag = ExternalTaskSensor(
        task_id='wait_for_avg_dag',
        external_dag_id='calculate_averages',
        external_task_id='end_task',  # Ожидает завершение всего DAG
        mode='reschedule',
        timeout=3600,
        poke_interval=30,
    )

    move_file_task = PythonOperator(
        task_id='move_to_processed',
        python_callable=move_to_processed,
        provide_context=True,
    )

    # Логика выполнения
    scan_and_trigger_task >> check_file_name_task
    check_file_name_task >> skip_processing
    check_file_name_task >> trigger_temp_dag >> wait_for_temp_dag
    check_file_name_task >> trigger_salinity_dag >> wait_for_salinity_dag
    check_file_name_task >> trigger_avg_dag >> wait_for_avg_dag
    [wait_for_temp_dag, wait_for_salinity_dag, wait_for_avg_dag] >> move_file_task
