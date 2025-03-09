-- silver_layer_transformation.sql
CREATE OR REPLACE TABLE `server-metrics-project.silver_layer.validated_server_metrics`
AS
SELECT
  server_id,
  server_name,
  server_type,
  location,
  TIMESTAMP(timestamp) AS timestamp,
  CASE
    WHEN cpu_usage >= 0 AND cpu_usage <= 100 THEN cpu_usage
    ELSE NULL
  END AS cpu_usage,
  CASE
    WHEN memory_usage >= 0 AND memory_usage <= 100 THEN memory_usage
    ELSE NULL
  END AS memory_usage,
  CASE
    WHEN disk_io >= 0 THEN disk_io
    ELSE NULL
  END AS disk_io,
  CASE
    WHEN network_in >= 0 THEN network_in
    ELSE NULL
  END AS network_in,
  CASE
    WHEN network_out >= 0 THEN network_out
    ELSE NULL
  END AS network_out,
  TIMESTAMP_DIFF(TIMESTAMP(ingest_timestamp), TIMESTAMP(timestamp), MILLISECOND) AS processing_delay_ms,
  CASE
    WHEN cpu_usage > 90 OR memory_usage > 90 THEN TRUE
    ELSE FALSE
  END AS is_high_utilization,
  EXTRACT(DATE FROM TIMESTAMP(timestamp)) AS metric_date,
  EXTRACT(HOUR FROM TIMESTAMP(timestamp)) AS hour_of_day,
  TIMESTAMP(ingest_timestamp) AS ingest_timestamp,
  CURRENT_TIMESTAMP() AS silver_processing_timestamp
FROM
  `server-metrics-project.bronze_layer.raw_server_metrics`
WHERE
  timestamp IS NOT NULL
  AND server_id IS NOT NULL
  AND (cpu_usage IS NULL OR (cpu_usage >= 0 AND cpu_usage <= 100))
  AND (memory_usage IS NULL OR (memory_usage >= 0 AND memory_usage <= 100))
  AND (disk_io IS NULL OR disk_io >= 0)
  AND (network_in IS NULL OR network_in >= 0)
  AND (network_out IS NULL OR network_out >= 0)