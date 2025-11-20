# Кастомный образ Airflow с установленным компилятором gcc
FROM apache/airflow:2.5.0-python3.8

# Переключаемся на пользователя root для установки пакетов
USER root

# Устанавливаем build-essential, который содержит gcc, g++ и другие утилиты
RUN apt-get update && apt-get install -y build-essential

# Возвращаемся к безопасному пользователю airflow
USER airflow
