import json
import os
from sys import prefix
import pandas as pd

from google.cloud import storage

def list_states_from_blobs(bucket_name, prefix="", project_id=None):
    client = storage.Client(project=project_id)
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    state_set = set()
    year_set = set()
    quater_set = set()
    for blob in blobs:
        parts = blob.name.split('/')
        # Expecting: ['pulse-data', 'aggregated', 'user', 'country', 'india', 'state', '<state-name>', '<year>', 'file.json']
        if len(parts) > 6:
            state_set.add(parts[6])  # Index 6 is the state name
        if len(parts) > 7:
            year_set.add(parts[7])  # Index 7 is the year, if needed
        if len(parts) > 8:
            quater_set.add(parts[8])  # Index 8 is the quater, if needed
    return list(sorted(state_set)), list(sorted(year_set)), list(sorted(quater_set))


bucket_name = "phonepe-insight-transaction"
prefix = "pulse-data/aggregated/user/country/india/state/"
project_id = "424692832551"
states, years, quarters = list_states_from_blobs(bucket_name, prefix=prefix, project_id=project_id)

clm = {'States': [],'Years': [],'Quarter': [],'Brand': [],'Transaction_count': [],'Transaction_percentage': []}


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
            users_by_device = D.get('data', {}).get('usersByDevice')
            if users_by_device:
                for z in users_by_device:
                    Brand = z.get('brand')
                    Count = z.get('count')
                    Percentage = z.get('percentage')
                    clm['Brand'].append(Brand)
                    clm['Transaction_count'].append(Count)
                    clm['Transaction_percentage'].append(Percentage)
                    clm['States'].append(i)  # i = state name
                    clm['Years'].append(j)   # j = year
                    clm['Quarter'].append(int(k.strip('.json')))  # k = quarter filename
# Convert to DataFrame
Agg_user = pd.DataFrame(clm)

# replacing the state names
Agg_user["States"] = Agg_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
Agg_user["States"] = Agg_user["States"].str.replace("-"," ")
Agg_user["States"] = Agg_user["States"].str.title()
Agg_user['States'] = Agg_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
Agg_user.to_csv("agg_user.csv", index=False)
