import datetime
import time
import os
import glob
import os
import json
import zipfile
import shutil

import pandas as pd
import lightgbm as lgb
import numpy as np

from nilearn.signal import clean
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix
from tsfresh import extract_features
from tsfresh.feature_extraction import EfficientFCParameters

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context


# aws constants - change this if needed
BUCKET_NAME = "awesome-airflow-bucket"
CATEGORY = "AOMIC_mini"  # AOMIC_copy = large, AOMIC = medium, AOMIC_mini = small


# dataset constants
DATA_FILE_NAME = f"{CATEGORY}.zip"
DATASET_DIR = "/mnt/shared"
EXTRACTED_ZIP_DIR = f"{DATASET_DIR}/{CATEGORY}"
TS_DIR = f"{EXTRACTED_ZIP_DIR}/TS"
NOISE_DIR = f"{EXTRACTED_ZIP_DIR}/Noise"
DATASET_FILE_SUFFIX = "_acq-seq_desc-confounds_regressors_6_motion_and_derivs.txt"
LABELS_DICT = {
    "task-restingstate": 0,
    "task-stopsignal": 1,
    "task-workingmemory": 2,
    "task-emomatching": 3,
}


session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    aws_session_token=os.environ["AWS_SESSION_TOKEN"],
)

s3 = session.client("s3")


def task_id():
    return get_current_context()["task_instance"].task_id


def model_name():
    # for task_id with value "train_model-1" return "model-1"
    return task_id().split("_")[1]


def create_dataset():
    zip_file_path = f"{DATASET_DIR}/data.zip"
    if not os.path.exists(EXTRACTED_ZIP_DIR):
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(DATASET_DIR)

    dataset = []
    labels = []

    data_files_paths = [i for i in glob.glob(f"{TS_DIR}/sub-*.txt")]
    data_files_paths.sort()
    data_files_names = [i.split("/")[-1] for i in data_files_paths]

    for data_file_path, data_file_name in zip(data_files_paths, data_files_names):
        # load dataset element
        data = np.loadtxt(data_file_path)

        file_name_parts = data_file_name.split("_")
        data_id = "_".join(file_name_parts[:2])

        # clean the element (denoise, detrend, standardize)
        noise = np.loadtxt(f"{NOISE_DIR}/{data_id}{DATASET_FILE_SUFFIX}")
        cleaned_data = clean(data, confounds=noise, standardize=True, detrend=True)
        dataset.append(cleaned_data)

        # get label
        label_key = file_name_parts[1]
        labels.append(LABELS_DICT[label_key])

    # make all elements of the dataset the same length (some time series are longer than others)
    shortest_data_len = min([len(item) for item in dataset])
    dataset = [item[:shortest_data_len] for item in dataset]

    return np.array(dataset), np.array(labels)


def split(dataset, labels):
    return train_test_split(dataset, labels, test_size=0.2, random_state=42)


def create_dataframe(dataset):
    ids = np.array(
        [[id_value] * dataset.shape[1] for id_value in range(dataset.shape[0])]
    )
    ids = ids.reshape(-1)

    df_values = dataset.reshape(-1, dataset.shape[2])
    df = pd.DataFrame(
        df_values, columns=[f"region{i}" for i in range(df_values.shape[1])]
    )
    df["ids"] = ids

    # make ids the first column
    df = df[["ids"] + [c for c in df if c not in ["ids"]]]

    return df


def calculate_features(df):
    return extract_features(
        df[:],
        column_id="ids",
        default_fc_parameters=EfficientFCParameters(),
        column_value="region0",
        n_jobs=0,
    )


def evaluate_model():
    # load model
    classifier = lgb.Booster(model_file=f"{DATASET_DIR}/{model_name()}.txt")

    # load test data
    test_features = pd.read_csv(f"{DATASET_DIR}/test_features.csv")
    y_test = pd.read_csv(f"{DATASET_DIR}/y_test.csv").to_numpy()

    # make predictions
    y_pred = classifier.predict(test_features)
    y_pred = np.argmax(y_pred, axis=1)

    accuracy = accuracy_score(y_test, y_pred, normalize=True)
    accuracy_dict = {"accuracy": accuracy}
    # save as json
    with open(f"{DATASET_DIR}/{model_name()}_accuracy.json", "w") as f:
        json.dump(accuracy_dict, f)


@task(task_id="download_the_data")
def download_data_task():
    s3.download_file(BUCKET_NAME, DATA_FILE_NAME, f"{DATASET_DIR}/data.zip")


@task(task_id="clean_and_prepare_the_data")
def prepare_data_task():
    dataset, labels = create_dataset()
    x_train, x_test, y_train, y_test = split(dataset, labels)

    df_train = create_dataframe(x_train)
    df_test = create_dataframe(x_test)

    train_features = calculate_features(df_train)
    test_features = calculate_features(df_test)

    # needed to make lightgbm work
    train_features.columns = [i for i in range(train_features.shape[1])]
    test_features.columns = [i for i in range(test_features.shape[1])]

    # save data without index
    train_features.to_csv(f"{DATASET_DIR}/train_features.csv", index=False)
    test_features.to_csv(f"{DATASET_DIR}/test_features.csv", index=False)

    # save labels
    pd.DataFrame(y_train).to_csv(f"{DATASET_DIR}/y_train.csv", index=False)
    pd.DataFrame(y_test).to_csv(f"{DATASET_DIR}/y_test.csv", index=False)


@task(task_id="train_model-1")
def train_model_one():
    # read data
    train_features = pd.read_csv(f"{DATASET_DIR}/train_features.csv")

    y_train = pd.read_csv(f"{DATASET_DIR}/y_train.csv")
    classifier = lgb.LGBMClassifier(num_leaves=31)
    classifier.fit(train_features, y_train)
    classifier.booster_.save_model(f"{DATASET_DIR}/{model_name()}.txt")


@task(task_id="train_model-2")
def train_model_two():
    train_features = pd.read_csv(f"{DATASET_DIR}/train_features.csv")
    y_train = pd.read_csv(f"{DATASET_DIR}/y_train.csv")

    classifier = lgb.LGBMClassifier(num_leaves=32)
    classifier.fit(train_features, y_train)

    classifier.booster_.save_model(f"{DATASET_DIR}/{model_name()}.txt")


@task(task_id="train_model-3")
def train_model_three():
    train_features = pd.read_csv(f"{DATASET_DIR}/train_features.csv")
    y_train = pd.read_csv(f"{DATASET_DIR}/y_train.csv")

    classifier = lgb.LGBMClassifier(num_leaves=33)
    classifier.fit(train_features, y_train)

    classifier.booster_.save_model(f"{DATASET_DIR}/{model_name()}.txt")


@task(task_id="evaluate_model-1")
def evaluate_model_1():
    evaluate_model()


@task(task_id="evaluate_model-2")
def evaluate_model_2():
    evaluate_model()


@task(task_id="evaluate_model-3")
def evaluate_model_3():
    evaluate_model()


@task(task_id="log_result")
def log_result():
    with open("/mnt/shared/logfile.txt", "w") as f_dest:
        for filename in os.listdir(DATASET_DIR):
            file_path = os.path.join(DATASET_DIR, filename)
            if file_path.endswith("_accuracy.json"):
                with open(file_path) as f_src:
                    f_dest.write(f"File {file_path}:\n")
                    f_dest.write(f"{f_src.read()}\n")
    upload_to_s3("logfile.txt")


@task(task_id="upload_best_model_to_s3")
def save_result():
    accuracy_files = glob.glob(f"{DATASET_DIR}/*_accuracy.json")
    accuracy_files.sort()
    file_names = [i.split("/")[-1] for i in accuracy_files]

    best_acc = -1
    best_model_name = None
    for i in range(len(accuracy_files)):
        file = accuracy_files[i]
        with open(file) as f:
            accuracy_dict = json.load(f)
            model_name = file_names[i].split("_")[0]
            accuracy = accuracy_dict["accuracy"]
            if accuracy > best_acc:
                best_acc = accuracy
                best_model_name = model_name

    upload_to_s3(f"{best_model_name}.txt")


@task(task_id="cleanup")
def cleanup():
    for filename in os.listdir(DATASET_DIR):
        file_path = os.path.join(DATASET_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")


def upload_to_s3(filename):
    s3.upload_file(f"/mnt/shared/{filename}", BUCKET_NAME, filename)


with DAG(
    dag_id="ml_pipeline_with_cleanup",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
):
    download_task = download_data_task()
    prepare_task = prepare_data_task()
    train_task1 = train_model_one()
    train_task2 = train_model_two()
    train_task3 = train_model_three()
    evaluate_task1 = evaluate_model_1()
    evaluate_task2 = evaluate_model_2()
    evaluate_task3 = evaluate_model_3()
    log_task = log_result()
    save_task = save_result()
    cleanup_task = cleanup()

    download_task >> prepare_task >> [train_task1, train_task2, train_task3]
    train_task1 >> evaluate_task1 >> [log_task, save_task]
    train_task2 >> evaluate_task2 >> [log_task, save_task]
    train_task3 >> evaluate_task3 >> [log_task, save_task]
    log_task >> cleanup_task
    save_task >> cleanup_task

with DAG(
    dag_id="ml_pipeline",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
):
    download_task = download_data_task()
    prepare_task = prepare_data_task()
    train_task1 = train_model_one()
    train_task2 = train_model_two()
    train_task3 = train_model_three()
    evaluate_task1 = evaluate_model_1()
    evaluate_task2 = evaluate_model_2()
    evaluate_task3 = evaluate_model_3()
    log_task = log_result()
    save_task = save_result()

    download_task >> prepare_task >> [train_task1, train_task2, train_task3]
    train_task1 >> evaluate_task1 >> [log_task, save_task]
    train_task2 >> evaluate_task2 >> [log_task, save_task]
    train_task3 >> evaluate_task3 >> [log_task, save_task]

with DAG(
    dag_id="cleanup",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
):
    cleanup_task = cleanup()
    cleanup_task
