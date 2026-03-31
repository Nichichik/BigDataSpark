# ETL реализованный с помощью Spark (Лабораторная работа №2)

## Описание

Проект реализует ETL-пайплайн с использованием Apache Spark:
1.  **Извлечение (Extract)**: Исходные CSV-данные (10 файлов по 1000 строк) загружаются в таблицу `mock_data` в **PostgreSQL**.
2.  **Трансформация (Transform)**: Spark преобразует денормализованные данные из `mock_data` в модель **"Звезда"** (`fact_sales`, `dim_customers`, `dim_products`, `dim_stores`, `dim_suppliers`) также в **PostgreSQL**.
3.  **Загрузка (Load)**: Spark генерирует 6 аналитических отчетов (витрин данных) на основе модели "Звезда" и загружает их в **ClickHouse**.

## Структура проекта

```text
.
├ docker-compose.yml              # Описание инфраструктуры: PostgreSQL, ClickHouse, Spark
├ scripts                         # SQL-скрипты для инициализации PostgreSQL
│   ├ ddl.sql                  # Создание таблицы mock_data
│   └ dml.sql                  # Загрузка CSV в mock_data
├ исходные данные                 # Директория с 10 CSV-файлами MOCK_DATA(*).csv
├ spark_etl_star.py               # Spark: ETL из raw_data в Star Schema (PostgreSQL)
├ spark_clickhouse_reports.py     # Spark: Генерация 6 отчетов в ClickHouse
└ README.md                       # Данный файл
```

## Используемые технологии

*   PostgreSQL 15
*   Apache Spark 3.5.1 (PySpark)
*   ClickHouse
*   Docker / Docker Compose
*   DBeaver (для проверки)

## Инструкция по запуску

1.  **Docker Desktop долженн быть запущен**
2.  **Перейдите в корневую директорию проекта** в терминале.
3.  **Запустите всю инфраструктуру и ETL-пайплайн**:
    ```bash
    docker-compose up -d --force-recreate
    ```
    *   Docker поднимет базы данных и Spark.
    *   PostgreSQL автоматически загрузит данные из CSV (`scripts/dml.sql`).
    *   Spark-задача (`spark-etl-job`) автоматически выполнит `spark_etl_star.py` и `spark_clickhouse_reports.py`.

4.  **Отслеживание выполнения**:
    Для просмотра логов Spark-задачи:
    ```bash
    docker logs -f spark-etl-job
    ```

5.  **Полный сброс**:
    Для очистки всех данных и сброса баз к исходному состоянию:
    ```bash
    docker-compose down -v
    ```

## Проверка результатов ETL

Используйте DBeaver для подключения и проверки.

### 1. Проверка PostgreSQL (Модель "Звезда")

*   **Подключение**:
    *   Host: `localhost`, Port: `5433`, Database: `lab_db`, User: `spark_user`, Password: `spark_password`
*   **Проверка**:
    *   Таблица `mock_data` должна содержать `10000` строк.
    *   Таблицы `dim_customers`, `dim_products`, `dim_stores`, `dim_suppliers`, `fact_sales` должны быть созданы.
    *   Таблица `fact_sales` должна содержать `10000` строк.
    *   Выполните SQL-запросы для проверки корректности связей и данных в таблицах `dim_` и `fact_sales`.

### 2. Проверка ClickHouse

*   **Подключение**:
    *   Host: `localhost`, Port: `8123`, Database: `reports_db`, User: `spark_user`, Password: `spark_password`
  *   **Проверка**:
      *   Должны быть созданы 6 таблиц-отчетов: `report_products`, `report_customers`, `report_time`, `report_stores`, `report_suppliers`, `report_quality`.
      *   Каждая таблица-отчет должна содержать агрегированные данные.
      *   Выполните SQL-запросы для просмотра данных в каждой витрине 
      Например:
      ```sql 
        SELECT count(*) FROM report_customers;
        SELECT customer_first_name, customer_last_name, total_spent, orders_count, average_check
        FROM report_customers
        ORDER BY total_spent DESC
        LIMIT 10;
        ```
