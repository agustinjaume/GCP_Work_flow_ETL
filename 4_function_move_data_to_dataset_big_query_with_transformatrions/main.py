from google.cloud import bigquery
from google.cloud import kms
import os
import re
import datetime
import json 
import base64
import pandas
import time

project_id = "ivory-honor-272915"
location_id = "europe-west3"
key_ring_id = "llavero_function_big_data"
key_id      = "llave_bigdata"

def create_dataset_and_table_and_move_data_encrypt_bq(data, context):
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
    print(project_id)
    print("move_bucket_to_big_query")
    move_bucket_to_big_query(client, dataset_name_only_30, file_name)
    print("get data with query SQL")
    get_data(project_id, dataset_name_only_30)    
    print("Add column to table")
    modify_table(project_id, dataset_name_only_30)
    print("transformation_data_encrypt")
    transformation_data(project_id, dataset_name_only_30, location_id, key_ring_id, key_id)


def move_bucket_to_big_query(client,dataset_name_only_30, file_name):
    print("dataset_name_only_30" + dataset_name_only_30)
    dataset = dataset_name_only_30 
    dataset_ref = client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]

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

def modify_table(project_id, dataset_name_only_30):
    client = bigquery.Client()
    #  table_id = "your-project.your_dataset.your_table_name"
    path_table = project_id + "." + dataset_name_only_30 + "." + dataset_name_only_30
    print(path_table)    
    table = client.get_table(path_table)  # Make an API request.
    original_schema = table.schema
    new_schema = original_schema[:]  # Creates a copy of the schema.
    new_schema.append(bigquery.SchemaField("c_encrypt", "STRING"))
    table.schema = new_schema
    table = client.update_table(table, ["schema"])  # Make an API request.
    if len(table.schema) == len(original_schema) + 1 == len(new_schema):
        print("A new column has been added.")
    else:
        print("The column has not been added.")

def encrypt_symmetric(project_id, location_id, key_ring_id, key_id, plaintext):
    plaintext_bytes = plaintext.encode('utf-8')# Convert the plaintext to bytes.  
    if 'key_name' in locals():  
        print("key exist")    
    else:
        client = kms.KeyManagementServiceClient()# Create the client.
        key_name = client.crypto_key_path(project_id, location_id, key_ring_id, key_id) # Build the key name.
    
    encrypt_response = client.encrypt(request={'name': key_name, 'plaintext': plaintext_bytes})# Call the API.
    time.sleep(1)
    #print('Ciphertext: {}'.format(base64.b64encode(encrypt_response.ciphertext)))
    return encrypt_response.ciphertext
   # return encrypt_response
    
def transformation_data(project_id, dataset_name_only_30,location_id, key_ring_id, key_id):
       
    path_table = project_id + "." + dataset_name_only_30 + "." + dataset_name_only_30
    print(path_table)
    sql = "SELECT count, name, nif, address, cp, color, phone, ssn, count_bank, c_encrypt FROM " + "`" + path_table + "`" + " LIMIT 100" 
    df = pandas.read_gbq(sql, dialect='standard')
    df = pandas.read_gbq(sql, project_id=project_id, dialect='standard')
    for nif, count_bank in zip(df['nif'], df['count_bank']) :
        plaintext = count_bank
        result = encrypt_symmetric(project_id, location_id, key_ring_id, key_id, plaintext)
        result_encode = str(base64.b64encode(result))
        print(str(nif))
        sql = "UPDATE " + "`" + path_table + "`" + " SET " + "c_encrypt = "  + "\"" + result_encode + "\"" + " WHERE" + " nif " + " LIKE " + "\"" + nif + "\""
        print(sql)
        time.sleep(2)
        df = pandas.read_gbq(sql, dialect='standard')
        df = pandas.read_gbq(sql, project_id=project_id, dialect='standard')
    print("End of sql query")
   



