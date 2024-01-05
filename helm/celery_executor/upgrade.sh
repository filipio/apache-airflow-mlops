helm template airflow apache-airflow/airflow --create-namespace --namespace airflow --values ../values.yaml --values ../common-override-values.yaml --values ./override-values.yaml --output-dir ./generated_yamls
helm upgrade airflow apache-airflow/airflow --namespace airflow --values ../values.yaml --values ../common-override-values.yaml --values ./override-values.yaml