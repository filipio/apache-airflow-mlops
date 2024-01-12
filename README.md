# apache-airflow-mlops

## Requirements

- `python3` installed
- `kind` installed
- `kubectl` installed
- `helm` installed

## Setup

1. Launch AWS learner academy and upload unzipped dataset files to S3 bucket named `airflow-mlops-dataset`
2. Paste aws learner academy credentials under `<project_dir>/secrets/aws_academy_credentials`
3. Run below command to setup kind cluster, k8s resources and airflow (during setup choose k8s/celery executor):

```bash
python3 setup.py
```

## Access

Run below command to get airflow webserver url under 8080:

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```

## Upgrade airflow

Do this if there is a new config or docker image has changed

```bash
python3 upgrade.py
```

## (Optional) Kubernetes Dashboard UI

```bash
kubectl apply -f ./k8s_resources/unlocked_dashboard.yaml
kubectl proxy
```

The dashboard is available at `http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/`, skip the login if prompted.

## Clean up

Run below command to delete kind cluster and k8s resources:

```bash
python3 cleanup.py
```
