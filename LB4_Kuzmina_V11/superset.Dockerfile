# Начинаем с официального образа Superset
FROM apache/superset:3.1.1

# Переключаемся на пользователя root для установки пакетов
USER root

# Устанавливаем драйвер для PostgreSQL
RUN pip install psycopg2-binary

# Возвращаемся к стандартному пользователю superset
USER superset