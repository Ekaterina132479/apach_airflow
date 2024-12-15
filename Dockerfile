# Базовый образ Airflow
FROM apache/airflow:latest

# Установка дополнительных Python-библиотек
RUN pip install matplotlib seaborn pandas
