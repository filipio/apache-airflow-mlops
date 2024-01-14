# NOTE: this scripts assumes kubectl forward is running at port 8080

import requests
import time
import json

from requests.auth import HTTPBasicAuth
from dateutil.parser import parse

dag_runs_time_in_seconds = []
auth = HTTPBasicAuth("admin", "admin")
total_dag_runs = 10
executor_type = "celery"  # for results file name, set to 'celery' or 'k8s'

for i in range(total_dag_runs):
    dag_run_scheduled = False
    while not dag_run_scheduled:
        try:
            start_dag_response = requests.post(
                "http://localhost:8080/api/v1/dags/ml_pipeline/dagRuns",
                auth=auth,
                json={},
            )

            payload = start_dag_response.json()
            print(f"dag run scheduled, response:")
            print(payload)
            dag_run_id = payload["dag_run_id"]
            dag_run_scheduled = True
        except:
            print("Connection refused, retrying to schedule dag run...")
            time.sleep(1)

    while True:
        try:
            dag_run_response = requests.get(
                f"http://localhost:8080/api/v1/dags/ml_pipeline/dagRuns/{dag_run_id}",
                auth=auth,
            )
            print("successfully got dag run info, response:")
            payload = dag_run_response.json()
            print(payload)
            if payload["state"] == "success":
                # calculate execution time
                start_date = parse(payload["start_date"])
                end_date = parse(payload["end_date"])
                # calculate time in seconds
                time_in_seconds = (end_date - start_date).seconds
                print("#" * 80)
                print(
                    f"dag run {dag_run_id} finished, execution time: {time_in_seconds} seconds"
                )
                print("#" * 80)
                dag_runs_time_in_seconds.append(time_in_seconds)
                break
            elif payload["state"] == "failed":
                print("#" * 80)
                print(f"dag run {dag_run_id} failed")
                print("#" * 80)
                break
            else:
                print("DAG run is not finished yet, waiting 5 seconds...")
                time.sleep(5)
        except:
            print("Connection refused, retrying to get dag run status...")
            time.sleep(1)

# save results to json file
with open(f"{executor_type}_results.json", "w") as f:
    json.dump({"dag_run_times_in_seconds": dag_runs_time_in_seconds}, f)
