from google.cloud import pubsub_v1  # pip install google-cloud-pubsub  ##to install
import glob  # for searching for json file
import ast
import os

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0];

# Set the project_id with your project ID
project_id = "pub-and-sub-449117";
topic_name = "design";  # change it for your topic name if needed
subscription_id = "design-sub";  # change it for your topic name if needed

# create a subscriber to the subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
topic_path = 'projects/{}/topics/{}'.format(project_id, topic_name);

print(f"Listening for messages on {subscription_path}..\n")

# Function to print key value pairs of the dictionary items
def print_dict(dictionary):
    print("Printing dictionary...")
    for key, value in dictionary.items():
        print(f'{key}: {value}')
    print()

# A callback function for handling received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # convert from bytes to string (deserialization)
    message_data = str(message.data.decode('utf-8'));
    # Converts the message from a string to a dictionary
    item  = ast.literal_eval(message_data)
    print("Consuming a record...")

    # Calls function to print dictionary items nicely
    print_dict(item)

    # Report To Google Pub/Sub the successful processed of the received messages
    message.ack()


with subscriber:
    # The call back function will be called for each message recieved from the topic
    # throught the subscription.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

