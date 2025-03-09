-- gold_layer_server_health.sql
CREATE OR REPLACE TABLE `server-metrics-project.gold_layer.server_health_summary`
AS
SELECT
  server_id,
  server_name,
  server_type,
  location,
  metric_date,
  AVG(cpu_usage) AS avg_daily_cpu,
  AVG(memory_usage) AS avg_daily_memory,
  MAX(cpu_usage) AS peak_cpu,
  MAX(memory_usage) AS peak_memory,
  COUNT(*) AS total_metrics,
  SUM(CASE WHEN is_high_utilization THEN 1 ELSE 0 END) AS high_utilization_minutes,
  SUM(CASE WHEN is_high_utilization THEN 1 ELSE 0 END) / COUNT(*) * 100 AS high_utilization_percentage,
  SUM(network_in) AS daily_network_in,
  SUM(network_out) AS daily_network_out,
  CURRENT_TIMESTAMP() AS gold_processing_timestamp,
  -- Server health score: 100 (best) to 0 (worst)
  100 - (AVG(cpu_usage) * 0.4 + AVG(memory_usage) * 0.4 + 
        (SUM(CASE WHEN is_high_utilization THEN 1 ELSE 0 END) / COUNT(*) * 100) * 0.2) AS server_health_score
FROM
  `server-metrics-project.silver_layer.validated_server_metrics`
GROUP BY
  server_id, server_name, server_type, location, metric_date
