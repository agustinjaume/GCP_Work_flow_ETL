from google.cloud import bigquery
from google.cloud import kms
import os
import re
import datetime
import json 
import base64
import pandas

project_id = "ivory-honor-272915"
location_id = "europe-west3"
key_ring_id = "llavero_function_big_data"
key_id      = "llave_bigdata"

def create_dataset_and_table_and_move_data_with_sql_query_bq(data, context):
    today = datetime.date.today()
    client = bigquery.Client()
    dataset_name = format(data['name'])
    file_name = format(data['name'])
    date = str(today) 
    # 30 characters, no spaces in the file name
    dataset_name_only = date + os.path.splitext(dataset_name)[0]
    dataset_name_only_clear = re.sub(r'[^a-zA-Z0-9_\s]+', '', dataset_name_only)
    dataset_name_only_30 = dataset_name_only_clear[0:30]  
    # 30 characters, no spaces in the file name
    print("name file:----------------------------------- " + file_name)
    print("name dataset:----------------------------------- " + dataset_name_only_30)
    dataset_id = "{}.{}" .format(client.project, dataset_name_only_30)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west3"
    dataset = client.create_dataset(dataset) 
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    # run function to move data from bucket to dataset
    move_bucket_to_data_query(client, dataset_name_only_30, file_name)
    # run function to move data from bucket to dataset
    print("data go to table Big query")
    print("get data")
    get_data(project_id, dataset_name_only_30)

def move_bucket_to_data_query(client,dataset_name_only_30, file_name):
    print("dataset_name_only_30" + dataset_name_only_30)
    dataset = dataset_name_only_30 
    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField("count", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("nif", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("cp", "STRING"),
        bigquery.SchemaField("color", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("ssn", "STRING"),
        bigquery.SchemaField("count_bank", "STRING"),
    ]
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://big-data-poblacion/" + str(file_name)

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table(dataset), job_config=job_config
    ) 
    print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table(dataset))
    print("Loaded {} rows.".format(destination_table.num_rows))

    


def get_data(project_id, dataset_name_only_30):

    path_table = project_id + "." + dataset_name_only_30 + "." + dataset_name_only_30
    print(path_table)
    sql = "SELECT name FROM " + "`" + path_table + "`" + " LIMIT 100"   
    #SELECT name  FROM `ivory-honor-272915.20200821db20200820100914tot10.20200821db20200820100914tot10` LIMIT 1000
    df = pandas.read_gbq(sql, dialect='standard')
    df = pandas.read_gbq(sql, project_id=project_id, dialect='standard')
    print(df)
    print("End of sql query")