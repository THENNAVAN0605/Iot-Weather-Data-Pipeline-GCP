CREATE TABLE IF NOT EXISTS `your-gcp-project-id.weather_dataset.weather_stream_data` (
  timestamp TIMESTAMP,
  temperature FLOAT64,
  humidity FLOAT64,
  location STRING
);
