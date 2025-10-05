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
        # Expecting: ['pulse-data', 'top', 'insurance','country', 'india', 'state', '<state-name>', '<year>', 'file.json']
        if len(parts) > 6:
            state_set.add(parts[6])  # Index 6 is the state name
        if len(parts) > 7:
            year_set.add(parts[7])  # Index 7 is the year, if needed
        if len(parts) > 8:
            quater_set.add(parts[8])  # Index 8 is the quater, if needed
    return list(sorted(state_set)), list(sorted(year_set)), list(sorted(quater_set))


bucket_name = "phonepe-insight-transaction"
prefix = "pulse-data/top/insurance/country/india/state/"
project_id = "424692832551"
states, years, quarters = list_states_from_blobs(bucket_name, prefix=prefix, project_id=project_id)

clm = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}
client = storage.Client(project=project_id)
bucket = client.bucket(bucket_name)
for state in states:
    for year in years:
        for quarter in quarters:
            blob_path = f"{prefix}{state}/{year}/{quarter}"
            blob = bucket.blob(blob_path)
            if not blob.exists():
                print(f"Skipping missing file: {blob_path}")
                continue
            try:
                data = blob.download_as_text()    
                D = json.loads(data)
                for z in D["data"]["pincodes"]:
                    entityName = z["entityName"]
                    count = z["metric"]["count"]
                    amount = z["metric"]["amount"]
                    clm["Pincodes"].append(entityName)
                    clm["Transaction_count"].append(count)
                    clm["Transaction_amount"].append(amount)
                    clm["States"].append(state)
                    clm["Years"].append(year)
                    clm["Quarter"].append(int(quarter.strip(".json")))
            except Exception as e:
                print(f"Error processing {blob_path}: {e}")
                    
# Convert to DataFrame
Top_insurance = pd.DataFrame(clm)

#replacing the state names
Top_insurance["States"] = Top_insurance["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
Top_insurance["States"] = Top_insurance["States"].str.replace("-"," ")
Top_insurance["States"] = Top_insurance["States"].str.title()
Top_insurance['States'] = Top_insurance['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
Top_insurance.to_csv("Top_insurance.csv", index=False)