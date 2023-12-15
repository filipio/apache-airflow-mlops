FROM apache/airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon
COPY ./dags ./dags
