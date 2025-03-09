# bronze_layer_consumer.py
import json
import datetime as dt
import argparse
from google.cloud import pubsub_v1
from google.cloud import bigquery
import os
from concurrent.futures import TimeoutError

# Set Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "metrics-pipeline-key.json"

# Parse command line arguments
parser = argparse.ArgumentParser(description='Bronze Layer Consumer')
parser.add_argument('--project_id', required=True, help='GCP Project ID')
parser.add_argument('--subscription_name', default='server-metrics-sub', 
                    help='Pub/Sub subscription name')
parser.add_argument('--batch_size', type=int, default=100, 
                    help='Number of messages to process in a batch')
args = parser.parse_args()

# Initialize BigQuery client
bq_client = bigquery.Client()
table_id = f"{args.project_id}.bronze_layer.raw_server_metrics"

# Initialize Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(args.project_id, args.subscription_name)

# Create a batch processor for BigQuery
rows_to_insert = []

def process_messages(message):
    try:
        # Process the message
        message_data = json.loads(message.data.decode('utf-8'))
        
        # Add ingest timestamp
        message_data['ingest_timestamp'] = dt.datetime.utcnow().isoformat()
        
        # Add to batch
        rows_to_insert.append(message_data)
        
        # If batch size is reached, insert to BigQuery
        if len(rows_to_insert) >= args.batch_size:
            insert_to_bigquery()
        
        # Acknowledge the message
        message.ack()
        
    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()  # Negative acknowledgment

def insert_to_bigquery():
    global rows_to_insert
    if not rows_to_insert:
        return
    
    try:
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Errors inserting rows: {errors}")
        else:
            print(f"Inserted {len(rows_to_insert)} rows to Bronze layer at {dt.datetime.utcnow()}")
        
        # Clear the batch
        rows_to_insert = []
    except Exception as e:
        print(f"Error inserting to BigQuery: {e}")

# Set up the subscriber
streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=process_messages,
    flow_control=pubsub_v1.types.FlowControl(max_messages=args.batch_size)
)

print(f"Listening for messages on {subscription_path}")

try:
    # Keep the consumer running
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    # Insert any remaining messages
    if rows_to_insert:
        insert_to_bigquery()
finally:
    subscriber.close()
