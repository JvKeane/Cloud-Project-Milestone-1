from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file
import json
import os
import csv

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="pub-and-sub-449117";
topic_name = "design";   # change it for your topic name if needed
subscription_id = "design-sub";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

csv_dict =  None

with open('Labels.csv', mode='r') as file:
    csv_dict =  list(csv.DictReader(file))
    total_rows = len(csv_dict)
    for i, item in enumerate(csv_dict):
        message = str(item).encode('utf-8')
        print(f'Producing a record {i + 1}/{total_rows}')
        future = publisher.publish(topic_path, message);
        future.result()
