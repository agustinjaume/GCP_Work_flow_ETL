from google.cloud import bigquery
import os
import re
import datetime
import json 

def create_dataset_and_table_and_move_data_bq(data, context):
    today = datetime.date.today()
    client = bigquery.Client()
    dataset_name = format(data['name'])
    file_name = format(data['name'])
    date = str(today) 
    # 30 characters, no spaces in the file name
    dataset_name_only = date + os.path.splitext(dataset_name)[0]
    dataset_name_only_clear = re.sub(r'[^a-zA-Z0-9_\s]+', '', dataset_name_only)
    dataset_name_only_20 = dataset_name_only_clear[0:30]  
    # 30 characters, no spaces in the file name
    print("name file:----------------------------------- " + file_name)
    print("name dataset:----------------------------------- " + dataset_name_only_20)
    dataset_id = "{}.{}" .format(client.project, dataset_name_only_20)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west3"
    dataset = client.create_dataset(dataset) 
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    # run function to move data from bucket to dataset
    move_bucketto_data_query(client, dataset_name_only_20, file_name)
    # run function to move data from bucket to dataset
    print("data go to table Big query")

def move_bucketto_data_query(client,dataset_name_only_20, file_name):
    print("dataset_name_only_20" + dataset_name_only_20)
    dataset = dataset_name_only_20 
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


