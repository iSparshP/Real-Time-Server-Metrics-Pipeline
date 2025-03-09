# Real-Time Server Metrics Pipeline with Medallion Architecture

## Architecture
![Image](https://github.com/user-attachments/assets/521f394b-03ce-474d-98c8-9013999fdc98)

## Overview
This project implements a comprehensive data engineering pipeline that processes high-frequency server metrics data through a medallion architecture (Bronze, Silver, Gold) on Google Cloud Platform and visualizes insights through an interactive Streamlit dashboard.

The system collects server performance metrics (CPU, memory, disk I/O, network traffic) at millisecond intervals, processes them through a structured data refinement pipeline, and provides analytical capabilities for monitoring server health and performance.

## Key Features
- **High-frequency data ingestion**: Processes server metrics generated at 15ms intervals
- **Medallion architecture**: Implements Bronze (raw), Silver (validated), and Gold (aggregated) data layers
- **Real-time streaming**: Uses Google Pub/Sub for reliable message delivery
- **Automated pipeline**: Cloud Scheduler ensures regular data processing
- **Interactive dashboard**: Streamlit visualization with filtering capabilities
- **Anomaly detection**: Identifies and reports high utilization events

## Technologies
- **Python**: Custom producer and consumer applications
- **Google Cloud Platform**:
  - **Pub/Sub**: Message streaming
  - **BigQuery**: Data storage and processing
  - **Cloud Functions**: Automated data transformations
  - **Cloud Scheduler**: Pipeline orchestration
  - **App Engine**: Dashboard hosting
- **Streamlit**: Interactive data visualization
- **Plotly**: Advanced charting capabilities

## Project Structure
```
server-metrics-pipeline/
├── src/
│   ├── server_metrics_producer.py        # High-frequency metrics generator
│   └── bronze_layer_consumer.py          # Pub/Sub to BigQuery ingestion
├── sql/
│   ├── silver_layer_transformation.sql   # Data validation and cleaning
│   ├── gold_layer_hourly_aggregates.sql  # Hourly metrics aggregation
│   ├── gold_layer_server_health.sql      # Server health scoring
│   └── gold_layer_location_performance.sql # Location-based analysis
├── dashboard/
│   ├── app.py                            # Streamlit dashboard application
│   ├── requirements.txt                  # Dashboard dependencies
│   └── app.yaml                          # App Engine configuration
├── cloud_function/
│   ├── main.py                           # Automated transformation function
│   └── requirements.txt                  # Function dependencies
└── README.md                             # Project documentation
```

## Setup Instructions

### Prerequisites
- Google Cloud Platform account
- Python 3.9+
- gcloud CLI installed

### Setting up GCP Environment
1. Create a new GCP project
   ```
   gcloud projects create server-metrics-project
   gcloud config set project server-metrics-project
   ```

2. Enable required APIs
   ```
   gcloud services enable compute.googleapis.com bigquery.googleapis.com pubsub.googleapis.com
   ```

3. Create service account
   ```
   gcloud iam service-accounts create metrics-pipeline-sa
   gcloud projects add-iam-policy-binding server-metrics-project \
       --member="serviceAccount:metrics-pipeline-sa@server-metrics-project.iam.gserviceaccount.com" \
       --role="roles/bigquery.dataEditor"
   gcloud projects add-iam-policy-binding server-metrics-project \
       --member="serviceAccount:metrics-pipeline-sa@server-metrics-project.iam.gserviceaccount.com" \
       --role="roles/bigquery.jobUser"
   gcloud projects add-iam-policy-binding server-metrics-project \
       --member="serviceAccount:metrics-pipeline-sa@server-metrics-project.iam.gserviceaccount.com" \
       --role="roles/pubsub.publisher"
   gcloud projects add-iam-policy-binding server-metrics-project \
       --member="serviceAccount:metrics-pipeline-sa@server-metrics-project.iam.gserviceaccount.com" \
       --role="roles/pubsub.subscriber"
   ```

4. Create key file
   ```
   gcloud iam service-accounts keys create metrics-pipeline-key.json \
       --iam-account=metrics-pipeline-sa@server-metrics-project.iam.gserviceaccount.com
   ```

### Setting up Pub/Sub
```
gcloud pubsub topics create server-metrics
gcloud pubsub subscriptions create server-metrics-sub --topic server-metrics
```

### Setting up BigQuery Datasets
```
bq mk --dataset server_metrics_project:bronze_layer
bq mk --dataset server_metrics_project:silver_layer
bq mk --dataset server_metrics_project:gold_layer
```

### Creating BigQuery Tables
```
bq mk --table --schema server_id:STRING,server_name:STRING,server_type:STRING,location:STRING,timestamp:TIMESTAMP,cpu_usage:FLOAT,memory_usage:FLOAT,disk_io:FLOAT,network_in:FLOAT,network_out:FLOAT,ingest_timestamp:TIMESTAMP server_metrics_project:bronze_layer.raw_server_metrics
```

## Running the Pipeline

### 1. Start the Producer
```
python src/server_metrics_producer.py --project_id server-metrics-project --topic_name server-metrics --num_servers 10 --frequency_ms 15
```

### 2. Start the Bronze Layer Consumer
```
python src/bronze_layer_consumer.py --project_id server-metrics-project --subscription_name server-metrics-sub --batch_size 100
```

### 3. Run Silver and Gold Layer Transformations
Execute SQL scripts for data transformations:
```
bq query --use_legacy_sql=false < sql/silver_layer_transformation.sql
bq query --use_legacy_sql=false < sql/gold_layer_hourly_aggregates.sql
bq query --use_legacy_sql=false < sql/gold_layer_server_health.sql
bq query --use_legacy_sql=false < sql/gold_layer_location_performance.sql
```

### 4. Run the Dashboard
```
cd dashboard
streamlit run app.py  # For local testing
```

## Automating the Pipeline
Deploy the Cloud Function for automated transformations:
```
cd cloud_function
gcloud functions deploy run_transformations --runtime python39 --trigger-http --allow-unauthenticated
```

Create a Cloud Scheduler job:
```
gcloud scheduler jobs create http transform-metrics \
    --schedule="*/15 * * * *" \
    --uri="[YOUR_CLOUD_FUNCTION_URL]" \
    --message-body="{}" \
    --time-zone="UTC"
```

## Dashboard Access
After running, access your dashboard at the localhost URL, typically:
http://localhost:8501/

## License
This project is licensed under the MIT License - see the LICENSE file for details.
