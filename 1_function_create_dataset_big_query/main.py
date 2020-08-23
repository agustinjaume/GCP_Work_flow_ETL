from google.cloud import bigquery
import os
import re
import datetime

def cp_file_to_bq(data, context):

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(data['bucket']))
    print('File: {}'.format(data['name']))
    print('Metageneration: {}'.format(data['metageneration']))
    print('Created: {}'.format(data['timeCreated']))
    print('Updated: {}'.format(data['updated']))

def create_dataset_and_table_bq(data, context):
    today = datetime.date.today()
    client = bigquery.Client()
    dataset_name = format(data['name'])
    date = str(today) 
    dataset_name_only = date + os.path.splitext(dataset_name)[0]
    dataset_name_only = re.sub(r'[^a-zA-Z0-9_\s]+', '', dataset_name_only)
    dataset_name_only_10 = dataset_name_only[0:20]  #"nombre_de_dataset"
    print("name file:----------------------------------- " + dataset_name_only_10)
    dataset_id = "{}.{}" .format(client.project, dataset_name_only_10)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west3"
    dataset = client.create_dataset(dataset)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
