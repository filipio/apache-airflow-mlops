import datetime
import time
import os
import pickle

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.multioutput import MultiOutputClassifier

from airflow import DAG
from airflow.decorators import task

@task(task_id="download_the_data")
def download_data_task():
    # TODO implement
    # Expected result: the file /mnt/shared/yelp_academic_dataset_business.json exists
    time.sleep(3)
    

@task(task_id="clean_and_prepare_the_data")
def prepare_data_task():
    # Preprocessing the businesses

    businesses = pd.read_json("/mnt/shared/yelp_academic_dataset_business.json", lines=True, orient='columns', chunksize=1000000, encoding='utf8', errors='ignore')
    # The data is huge, this takes only a subset
    for business in businesses:
        subset_business = business
        break

    # From the subset we only take restaurants in Toronto
    # Businesses in Toronto and currently open business
    city = subset_business[(subset_business['city'] == 'Toronto') & (subset_business['is_open'] == 1)]
    toronto = city[['business_id','name','address', 'categories', 'attributes','stars']]
    # From the buisinesses we only take restaurants
    rest = toronto[toronto['categories'].str.contains('Restaurant.*')==True].reset_index()

    # Update the restaurants frame to unpack the nested 'attributes' column fields
    rest['BusinessParking'] = rest.apply(lambda x: str_to_dict(extract_keys(x['attributes'], 'BusinessParking')), axis=1)
    rest['Ambience'] = rest.apply(lambda x: str_to_dict(extract_keys(x['attributes'], 'Ambience')), axis=1)
    rest['GoodForMeal'] = rest.apply(lambda x: str_to_dict(extract_keys(x['attributes'], 'GoodForMeal')), axis=1)
    rest['Dietary'] = rest.apply(lambda x: str_to_dict(extract_keys(x['attributes'], 'Dietary')), axis=1)
    rest['Music'] = rest.apply(lambda x: str_to_dict(extract_keys(x['attributes'], 'Music')), axis=1)

    # create table with attribute dummies
    df_attr = pd.concat([ rest['attributes'].apply(pd.Series), rest['BusinessParking'].apply(pd.Series),
                        rest['Ambience'].apply(pd.Series), rest['GoodForMeal'].apply(pd.Series), 
                        rest['Dietary'].apply(pd.Series) ], axis=1)
    df_attr_dummies = pd.get_dummies(df_attr)
    # get dummies from categories
    df_categories_dummies = pd.Series(rest['categories']).str.get_dummies(',')
    df_categories_dummies

    # pull out names and stars from rest table 
    result = rest[['name','stars']]

    # Concat all tables and drop Restaurant column
    df_final = pd.concat([df_attr_dummies, df_categories_dummies, result], axis=1)
    df_final.drop('Restaurants',inplace=True,axis=1)

    # map floating point stars to an integer
    mapper = {1.0:1,1.5:2, 2.0:2, 2.5:3, 3.0:3, 3.5:4, 4.0:4, 4.5:5, 5.0:5}
    df_final['stars'] = df_final['stars'].map(mapper)

    df_final.to_csv('/mnt/shared/data_clean.csv')

    # split for knn
    X_knn = df_final.iloc[:,:-2]
    y_knn = df_final['stars']
    X_train_knn, X_test_knn, y_train_knn, y_test_knn = train_test_split(X_knn, y_knn, test_size=0.2, random_state=1)
    X_train_knn.to_csv('/mnt/shared/data_X_train_knn.csv')
    X_test_knn.to_csv('/mnt/shared/data_X_test_knn.csv')
    y_train_knn.to_csv('/mnt/shared/data_y_train_knn.csv')
    y_test_knn.to_csv('/mnt/shared/data_y_test_knn.csv')

    # Preprocessing the reviews

    reviews = pd.read_json("/mnt/shared/yelp_academic_dataset_review.json", lines=True, orient='columns', chunksize=1000000, encoding='utf8', errors='ignore')
    for review in reviews:
        subset_review = review
        break

    # pull out needed columns from subset_review table
    df_review = subset_review[['user_id','business_id','stars', 'date']]
    # pull out names and addresses of the restaurants from rest table
    restaurant = rest[['business_id', 'name', 'address']]
    # combine df_review and restaurant table
    combined_business_data = pd.merge(df_review, restaurant, on='business_id')
    # create a user-item matrix
    rating_crosstab = combined_business_data.pivot_table(values='stars', index='user_id', columns='name', fill_value=0)
    # Transpose the Utility matrix
    X_svd = rating_crosstab.values.T
    X_svd.to_csv('/mnt/shared/data_X_train_svd.csv')

@task(task_id="train_model_1")
def train_model_one():
    # read and split data
    X_train_knn = pd.read_csv('/mnt/shared/data_X_train_knn.csv')
    y_train_knn = pd.read_csv('/mnt/shared/data_y_train_knn.csv')

    # train KNN
    # CUSTOM CHANGE - wrapped with MultiOutputClassifier
    knn = MultiOutputClassifier(KNeighborsClassifier(n_neighbors=20), n_jobs=-1)
    knn.fit(X_train_knn, y_train_knn)

    # save model
    with open('/mnt/shared/knn.pkl', 'wb') as knn_file:
        pickle.dump(knn, knn_file)  


@task(task_id="train_model_2")
def train_model_two():
    # TODO implement
    time.sleep(3)

@task(task_id="train_model_3")
def train_model_three():
    # TODO implement
    time.sleep(3)

@task(task_id="evaluate_model_1")
def evaluate_model_1():
    # load knn
    knn: MultiOutputClassifier = pickle.load(open('/mnt/shared/knn.pkl', 'rb'))
    X_test_knn = pd.read_csv('/mnt/shared/data_X_test_knn.csv')
    y_test_knn = pd.read_csv('/mnt/shared/data_y_test_knn.csv')

    # evaluate
    # accuracy_train = knn.score(X_train_knn, y_train_knn) # unused
    accuracy_test = knn.score(X_test_knn, y_test_knn)

    # save results
    with open('/mnt/shared/knn_eval.csv', 'w') as eval_file:
        eval_file.write(f'{accuracy_test}')

    return accuracy_test

@task(task_id="evaluate_model_2")
def evaluate_model_2():
    # TODO implement
    time.sleep(3)

@task(task_id="evaluate_model_3")
def evaluate_model_3():
    # TODO implement
    time.sleep(3)

@task(task_id="log_result")
def log_result():
    # TODO implement (make a nice log file and upload it to S3)
    time.sleep(3)

@task(task_id="save_result")
def save_result():
    # TODO implement (choose best model based on eval files and upload it to the cloud)
    # Eval files:
    # - /mnt/shared/knn_eval.csv contains a single float with the model's accracy
    # - TODO svm
    # - TODO neural network

    time.sleep(3)

@task(task_id="cleanup")
def cleanup():
    # current implementation wipes the whole persistant volume, except for the yelp data
    for subdir, dirs, files in os.walk('/mnt/shared'):
        for file in files:
            # don't remove the raw data
            if not (file.startswith('yelp' and file.endswith('.json'))):
                filepath = subdir + os.sep + file
                os.remove(filepath)

# The "attributes" column in the yelp data has nested attributes. 
# In order to create a feature table, we need to separate those nested attributes into their own columns. 
# Therefore, the two following functions will be used to achieve this goal.
# Function that extract keys from the nested dictionary
def extract_keys(attr, key):
    if attr == None:
        return "{}"
    if key in attr:
        return attr.pop(key)

# convert string to dictionary
import ast
def str_to_dict(attr):
    if attr != None:
        return ast.literal_eval(attr)
    else:
        return ast.literal_eval("{}")    

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
    cleanup_task = cleanup()
    
    download_task >> prepare_task >> [ train_task1, train_task2, train_task3 ]
    train_task1 >> evaluate_task1 >> [ log_task, save_task ]
    train_task2 >> evaluate_task2 >> [ log_task, save_task ]
    train_task3 >> evaluate_task3 >> [ log_task, save_task ]
    log_task >> cleanup_task
    save_task >> cleanup_task

with DAG(
    dag_id="ml_pipeline_no_cleanup",
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
 
    download_task >> prepare_task >> [ train_task1, train_task2, train_task3 ]
    train_task1 >> evaluate_task1 >> [ log_task, save_task ]
    train_task2 >> evaluate_task2 >> [ log_task, save_task ]
    train_task3 >> evaluate_task3 >> [ log_task, save_task ]

with DAG(
    dag_id="cleanup",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
):
    cleanup_task = cleanup()
    cleanup_task