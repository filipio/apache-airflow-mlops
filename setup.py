import configparser
import os

AWS_CONNECTION_ID = "my_aws_connection"  # use this in airflow files, see s3_dag.py
SECRETS_PATH = './secrets/aws_academy_credentials'  # store credentials here
K8S_RESOURCES_PATH = './k8s_resources'  # store k8s resources here

print("setting up kind cluster")
command = "kind create cluster --config ./kind-config.yaml"
os.system(command)

print("building docker image for airflow")
os.system("docker build -t my-dags:0.0.1 .")

print("loading docker image into kind cluster")
os.system("kind load docker-image my-dags:0.0.1")

print("creating airflow namespace")
command = "kubectl create namespace airflow"
os.system(command)

config = configparser.ConfigParser()
config.read(SECRETS_PATH)

aws_access_key_id = config.get('default', 'aws_access_key_id')
aws_secret_access_key = config.get('default', 'aws_secret_access_key')
aws_session_token = config.get('default', 'aws_session_token')

secret_name = AWS_CONNECTION_ID.replace("_", "-")
print(f"creating kubernetes secret {secret_name}")
command = f"kubectl create secret generic {secret_name} --from-literal=AWS_ACCESS_KEY_ID={aws_access_key_id} --from-literal=AWS_SECRET_ACCESS_KEY={aws_secret_access_key} --from-literal=AWS_SESSION_TOKEN={aws_session_token} --namespace airflow"
os.system(command)

print(f"creating kubernetes pvc")
command = f"kubectl apply -f {K8S_RESOURCES_PATH}/pvc.yaml"
os.system(command)

print("fetch airflow helm chart")
os.system("helm repo add apache-airflow https://airflow.apache.org")
os.system("helm repo update")

executor = input("Enter executor - celery(c) or k8s(k):")
if executor == "c":
    print("setting up airflow with celery executor")
    os.chdir("./helm/celery_executor")
    command = f"./install.sh"
    os.system(command)
elif executor == "k":
    print("setting up airflow with k8s executor")
    os.chdir("./helm/k8s_executor")
    command = f"./install.sh"
    os.system(command)