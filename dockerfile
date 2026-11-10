
FROM apache/airflow:3.3.0

COPY airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.10.txt" \
    -r /requirements.txt
