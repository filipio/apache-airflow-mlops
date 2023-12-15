# apache-airflow-mlops

## Requirements
- `kind` installed
- `kubectl` installed
- `helm` installed

## Setup
1. Create kubernetes cluster with 3 workers and 1 master:
```bash
kind create cluster --config kind-config.yaml
```
Verify that nodes have been created via `kubectl get nodes`

2. Create docker image with backed dags:
```bash
docker build -t my-dags:0.0.1 .
```

3. Upload image to kind cluster:
```bash
kind load docker-image my-dags:0.0.1
```

4. Install airflow using helm charts:
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
cd ./helm
chmod +x install.sh upgrade.sh
./install.sh
```

5. Forward airflow webserver port to localhost:
```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```

6. Enter `localhost:8080` in browser and login with credentials `admin:admin`

NOTE: for now it's needed to rebuild & reupload the docker image if there's any change in the DAG files.

## AWS
`s3_dag.py` shows sample DAG which lists keys that exist in S3 bucket. It uses S3 connection that needs to be created at first.
To do so:
1. Copy aws credentials values (access key, secret key, session key) to gaps in `aws_conn_uri_printer.py`
2. Run `python aws_conn_uri_printer.py` to get connection URI
3. Encode URI with base64 using
```bash
echo -n "<URI_HERE>" | base64
```
4. Paste the encoded URI to `aws_connection_secret.yaml`
5. Run `kubectl apply -f aws_connection_secret.yaml` to create secret
6. Uncomment additional secrets in `override-values.yaml` and run `./upgrade.sh` to apply changes
7. Create the bucket in AWS with the name the same as in `s3_dag.py` and verify the DAG is running correctly

