from google.cloud import storage
import json
import pandas as pd

def list_states_from_blobs(bucket_name, prefix="", project_id=None):
    client = storage.Client(project=project_id)
    blobs = client.list_blobs(bucket_name, prefix=prefix)

    state_set = set()
    year_set = set()
    quarter_set = set()
    for blob in blobs:
        parts = blob.name.split('/')
        if len(parts) > 6:
            state_set.add(parts[6])
        if len(parts) > 7:
            year_set.add(parts[7])
        if len(parts) > 8:
            quarter_set.add(parts[8])
    return sorted(state_set), sorted(year_set), sorted(quarter_set)

def blob_exists(bucket, blob_path):
    blob = bucket.blob(blob_path)
    return blob.exists()

# Set your bucket and project info
bucket_name = "phonepe-insight-transaction"
prefix = "pulse-data/aggregated/insurance/country/india/state/"
project_id = "424692832551"

states, years, quarters = list_states_from_blobs(bucket_name, prefix=prefix, project_id=project_id)

clm = {
    'States': [],
    'Years': [],
    'Quarter': [],
    'Transaction_type': [],
    'Transaction_count': [],
    'Transaction_amount': []
}

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
                for record in D['data'].get('transactionData', []):
                    name = record['name']
                    count = record['paymentInstruments'][0]['count']
                    amount = record['paymentInstruments'][0]['amount']
                    clm['Transaction_type'].append(name)
                    clm['Transaction_count'].append(count)
                    clm['Transaction_amount'].append(amount)
                    clm['States'].append(state)
                    clm['Years'].append(year)
                    clm['Quarter'].append(int(quarter.strip('.json')))
            except Exception as e:
                print(f"Error processing {blob_path}: {e}")

# Create DataFrame
Agg_Insurance = pd.DataFrame(clm)

# Clean up state names
Agg_Insurance["States"] = (
    Agg_Insurance["States"]
    .str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar", regex=False)
    .str.replace("-", " ")
    .str.title()
    .str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu", regex=False)
)

Agg_Insurance.to_csv("agg_insurance.csv", index=False)
print("File saved as agg_insurance.csv")
