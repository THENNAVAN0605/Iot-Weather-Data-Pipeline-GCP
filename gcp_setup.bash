gcloud pubsub topics create weather-data
bq mk weather_dataset
bq mk -t --schema temperature:FLOAT,humidity:FLOAT,timestamp:TIMESTAMP weather_dataset.weather_table
