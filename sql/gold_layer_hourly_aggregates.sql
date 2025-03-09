-- gold_layer_hourly_aggregates.sql
CREATE OR REPLACE TABLE `server-metrics-project.gold_layer.hourly_server_metrics`
AS
SELECT
  server_id,
  server_name,
  server_type,
  location,
  metric_date,
  hour_of_day,
  COUNT(*) AS num_metrics,
  AVG(cpu_usage) AS avg_cpu_usage,
  MAX(cpu_usage) AS max_cpu_usage,
  AVG(memory_usage) AS avg_memory_usage,
  MAX(memory_usage) AS max_memory_usage,
  AVG(disk_io) AS avg_disk_io,
  SUM(network_in) AS total_network_in,
  SUM(network_out) AS total_network_out,
  SUM(CASE WHEN is_high_utilization THEN 1 ELSE 0 END) AS high_utilization_count,
  CURRENT_TIMESTAMP() AS gold_processing_timestamp
FROM
  `server-metrics-project.silver_layer.validated_server_metrics`
GROUP BY
  server_id, server_name, server_type, location, metric_date, hour_of_day
