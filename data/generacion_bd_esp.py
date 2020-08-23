from faker import Faker
from datetime import datetime
import csv,  pandas, os
import json

fake = Faker(['es_ES']) 
total= 15  #1000000
time_file = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
destinationcsv="db-"+ str(time_file)+"-tot"+(str(total))+ ".csv"
destinationjson="db-"+ str(time_file)+"-tot"+(str(total))+ ".json"
def create_csv(total):
    data = []
    count = 0
    for _ in range(total):
        count +=1
        name = fake.name()
        address = fake.street_address()
        color = fake.color()
        phone = fake.phone_number()
        nif = fake.doi()
        ssn = fake.ssn()
        cp = "cp: " + fake.postcode()
        count_bank = "count_bank: " + fake.iban()
        data = [
                count, 
                name ,
                nif,
                address,
                cp, 
                color, 
                phone, 
                ssn,
                count_bank 
                ]
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)
        wr.writerow(data)
        print(data)

try:
    with open(destinationcsv,"w", encoding='UTF-8', newline='') as f:
        create_csv(total)   
    print('Se ha creado el archivo: ' + destinationcsv )
finally:
    print("DONE")
