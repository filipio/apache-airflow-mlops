import os
command = f"./upgrade.sh"

executor = input("Enter executor - celery(c) or k8s(k):")
if executor == "c":
    print("setting up airflow with celery executor")
    os.chdir("./helm/celery_executor")
    os.system(command)
elif executor == "k":
    print("setting up airflow with k8s executor")
    os.chdir("./helm/k8s_executor")
    os.system(command)