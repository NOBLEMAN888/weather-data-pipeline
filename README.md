## Weather Data Pipeline

Проект реализует ETL-пайплайн на ***Apache Airflow***, который ежедневно получает json с погодными данными с
*api.openweathermap.org*, преобразует в нормализованную форму и сохраняет в таблицу PostgreSQL.

Обмен данными между задачами происходит через XCom.

### Схема DAG в Apache Airflow:

flowchart TD
    A["get_weather<br/>(Extract)"] --> B["transform_data<br/>(Transform)<br/>→ XCom"]
    B --> C["load_to_postgres<br/>(Load)"]

### Технологии:

- Python
- Apache Airflow
- PostgreSQL
