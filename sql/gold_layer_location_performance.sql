-- gold_layer_location_performance.sql
CREATE OR REPLACE TABLE `server-metrics-project.gold_layer.location_performance`
AS
SELECT
  location,
  server_type,
  metric_date,
  COUNT(DISTINCT server_id) AS num_servers,
  AVG(cpu_usage) AS avg_cpu_usage,
  AVG(memory_usage) AS avg_memory_usage,
  AVG(disk_io) AS avg_disk_io,
  SUM(network_in) AS total_network_in,
  SUM(network_out) AS total_network_out,
  COUNT(CASE WHEN is_high_utilization THEN 1 END) AS high_utilization_count,
  COUNT(CASE WHEN is_high_utilization THEN 1 END) / COUNT(*) * 100 AS high_utilization_percentage,
  CURRENT_TIMESTAMP() AS gold_processing_timestamp
FROM
  `server-metrics-project.silver_layer.validated_server_metrics`
GROUP BY
  location, server_type, metric_date
