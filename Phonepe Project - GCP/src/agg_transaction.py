import json
import os
from sys import prefix
import pandas as pd
import pymysql
import requests

from google.cloud import storage

def list_states_from_blobs(bucket_name, prefix="", project_id=None):
    client = storage.Client(project=project_id)
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    state_set = set()
    year_set = set()
    quater_set = set()
    for blob in blobs:
        parts = blob.name.split('/')
        # Expecting: ['pulse-data', 'aggregated', 'transaction', 'country', 'india', 'state', '<state-name>', '<year>', 'file.json']
        if len(parts) > 6:
            state_set.add(parts[6])  # Index 6 is the state name
        if len(parts) > 7:
            year_set.add(parts[7])  # Index 7 is the year, if needed
        if len(parts) > 8:
            quater_set.add(parts[8])  # Index 8 is the quater, if needed
    return list(sorted(state_set)), list(sorted(year_set)), list(sorted(quater_set))


bucket_name = "phonepe-insight-transaction"
prefix = "pulse-data/aggregated/transaction/country/india/state/"
project_id = "424692832551"
states, years, quarters = list_states_from_blobs(bucket_name, prefix=prefix, project_id=project_id)
clm={'States':[], 'Years':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in states:
    p_i=prefix+i+"/"
    for j in years:
        p_j=p_i+j+"/"
        for k in quarters:
            p_k=p_j+k
            client = storage.Client(project=project_id)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(p_k)
            data = blob.download_as_text()
            D=json.loads(data)
            for z in D['data']['transactionData']:
                Name=z['name']
                count=z['paymentInstruments'][0]['count']
                amount=z['paymentInstruments'][0]['amount']
                clm['Transaction_type'].append(Name)
                clm['Transaction_count'].append(count)
                clm['Transaction_amount'].append(amount)
                clm['States'].append(i)
                clm['Years'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))
Agg_Trans=pd.DataFrame(clm)
Agg_Trans["States"] = Agg_Trans["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
Agg_Trans["States"] = Agg_Trans["States"].str.replace("-"," ")
Agg_Trans["States"] = Agg_Trans["States"].str.title()
Agg_Trans['States'] = Agg_Trans['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
Agg_Trans.to_csv("agg_trans.csv", index=False)


