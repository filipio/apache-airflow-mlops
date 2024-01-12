FROM apache/airflow
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         libgomp1 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
