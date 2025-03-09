# server_metrics_pubsub_producer.py
import json
import time
import random
import uuid
import datetime as dt
import argparse
from google.cloud import pubsub_v1
import os

# Set Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "metrics-pipeline-key.json"

# Parse command line arguments
parser = argparse.ArgumentParser(description='Server Metrics Producer for Pub/Sub')
parser.add_argument('--project_id', required=True, help='GCP Project ID')
parser.add_argument('--topic_name', default='server-metrics', help='Pub/Sub topic name')
parser.add_argument('--num_servers', type=int, default=5, help='Number of simulated servers')
parser.add_argument('--frequency_ms', type=int, default=15, help='Frequency in milliseconds')
args = parser.parse_args()

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(args.project_id, args.topic_name)

print(f'Initialized Pub/Sub publisher at {dt.datetime.utcnow()} for topic {topic_path}')

# Server names and types for simulation
server_types = ['web', 'database', 'application', 'cache', 'file']
server_locations = ['us-east', 'us-west', 'europe', 'asia', 'australia']
servers = []

# Generate server configurations
for i in range(args.num_servers):
    server_id = str(uuid.uuid4())
    server_type = random.choice(server_types)
    server_location = random.choice(server_locations)
    servers.append({
        'server_id': server_id,
        'server_name': f'{server_type}-server-{i:03d}',
        'server_type': server_type,
        'location': server_location
    })

# Generate and send metrics
try:
    while True:
        start_time = time.time()
        
        for server in servers:
            # Generate random metrics
            metrics = {
                'timestamp': dt.datetime.utcnow().isoformat(),
                'server_id': server['server_id'],
                'server_name': server['server_name'],
                'server_type': server['server_type'],
                'location': server['location'],
                'cpu_usage': round(random.uniform(0.1, 100.0), 2),
                'memory_usage': round(random.uniform(0.1, 100.0), 2),
                'disk_io': round(random.uniform(0, 5000), 2),
                'network_in': round(random.uniform(0, 10000), 2),
                'network_out': round(random.uniform(0, 10000), 2)
            }
            
            # Introduce occasional anomalies
            if random.random() < 0.01:  # 1% chance of an anomaly
                metrics['cpu_usage'] = round(random.uniform(95, 100), 2)
                metrics['memory_usage'] = round(random.uniform(95, 100), 2)
            
            # Convert metrics to JSON and encode as bytes
            message_data = json.dumps(metrics).encode('utf-8')
            
            # Publish message
            future = publisher.publish(topic_path, data=message_data, 
                                      server_id=server['server_id'])
        
        # Calculate and adjust sleep time to maintain frequency
        elapsed = (time.time() - start_time) * 1000  # convert to ms
        sleep_time = max(0, (args.frequency_ms - elapsed) / 1000)  # convert to seconds
        time.sleep(sleep_time)
        
        if random.random() < 0.005:  # Occasionally log progress
            print(f'Sent metrics at {dt.datetime.utcnow()}')
            
except KeyboardInterrupt:
    print('Producer stopped by user')
