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
        # Expecting: ['pulse-data', 'map', 'user', 'country', 'india', 'state', '<state-name>', '<year>', 'file.json']
        if len(parts) > 7:
            state_set.add(parts[7])  # Index 6 is the state name
        if len(parts) > 8:
            year_set.add(parts[8])  # Index 7 is the year, if needed
        if len(parts) > 9:
            quater_set.add(parts[9])  # Index 8 is the quater, if needed
    return list(sorted(state_set)), list(sorted(year_set)), list(sorted(quater_set))
bucket_name = "phonepe-insight-transaction"
prefix = "pulse-data/map/user/hover/country/india/state/"
project_id = "424692832551"
states, years, quarters = list_states_from_blobs(bucket_name, prefix=prefix, project_id=project_id)

clm = {
    "States": [],
    "Years": [],
    "Quarter": [],
    "District": [],
    "RegisteredUser": [],
    "AppOpens": []
}

for i in states:
    p_i=prefix+i+"/"
    for j in years:
        p_j=p_i+j+"/"
        for k in quarters:
            p_k=p_j+k
            client = storage.Client(project=project_id)
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(p_k)
            if not blob.exists():
                print(f"Skipping missing file: {p_k}")
                continue
            data = blob.download_as_text()
            D=json.loads(data)
            for district, district_data in D['data']['hoverData'].items():
                registereduser = district_data["registeredUsers"]
                appopens = district_data["appOpens"]
                clm["District"].append(district)
                clm["RegisteredUser"].append(registereduser)
                clm["AppOpens"].append(appopens)
                clm["States"].append(i)
                clm["Years"].append(j)
                clm["Quarter"].append(int(k.strip(".json")))

# Convert to DataFrame                    
map_user = pd.DataFrame(clm)

# replacing the state names
map_user["States"] = map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user['States'] = map_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
map_user.to_csv("map_user.csv", index=False)
