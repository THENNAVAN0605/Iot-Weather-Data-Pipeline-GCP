import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class ParseAndTransform(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        return [{
            'temperature': record['temperature'],
            'humidity': record['humidity'],
            'timestamp': record['timestamp']
        }]

options = PipelineOptions(
    streaming=True,
    project='your-project-id',
    region='your-region',
    runner='DataflowRunner',
    temp_location='gs://your-bucket/temp'
)

with beam.Pipeline(options=options) as p:
    (
        p
        | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic='projects/your-project-id/topics/weather-data')
        | 'ParseJson' >> beam.ParDo(ParseAndTransform())
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table='your-project-id:weather_dataset.weather_table',
            schema='temperature:FLOAT,humidity:FLOAT,timestamp:TIMESTAMP',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
