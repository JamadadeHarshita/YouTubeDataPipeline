import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.filesystems import FileSystems
import json

class ParseJsonArray(beam.DoFn):
    def process(self, element):
        """Parse and process JSON array."""
        try:
            # Load the entire JSON array from a string
            records = json.loads(element)
            if not isinstance(records, list):
                raise ValueError("Expected a JSON array, got something else")
            # Yield each record from the array
            for record in records:
                yield record
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing JSON array: {e}")
            return  # Skip invalid JSON arrays

class TransformJsonRecord(beam.DoFn):
    def process(self, record):
        """Transform each JSON record."""
        try:
            # Calculate average views
            total_views = int(record.get("total_views", 0))
            video_count = int(record.get("video_count", 0))
            avg_views = total_views / video_count if video_count > 0 else 0
            # Transform topics
            topics = [url.split('/')[-1] for url in record.get("topics", [])]
            # Yield the transformed record
            yield {
                "channel_id": record.get("channel_id"),
                "title": record.get("title", "Unknown"),
                "subscribers": int(record.get("subscribers", 0)),
                "total_views": total_views,
                "video_count": video_count,
                "description": record.get("description", "No Description").strip(),
                "country": record.get("country", "Unknown"),
                "thumbnail": record.get("thumbnail", "No Thumbnail"),
                "topics": ",".join(topics),  # Convert list to comma-separated string
                "avg_views": avg_views,
            }
        except Exception as e:
            print(f"Error processing record: {record} -> {e}")
            return  # Skip invalid records

def read_entire_file(file_path):
    """Read the entire content of a JSON file from GCS."""
    with FileSystems.open(file_path) as f:
        return f.read().decode("utf-8")

# Define pipeline options
class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input2', help='Input file path on GCS',
                             default="gs://sidemen-buckets1/youtube/channel_stats.json" )
        parser.add_argument('--output2', help='Output BigQuery table',
                            default = "youtube-analytics-444717:youtube_datasets1.channel_stats")

# Main pipeline
def run(pipeline_options = None):

    if pipeline_options is None:
        pipeline_options = PipelineOptions()

    options = pipeline_options.view_as(MyOptions)
    # Define BigQuery schema
    table_schema = {
        "fields": [
            {"name": "channel_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "subscribers", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "total_views", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "video_count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "thumbnail", "type": "STRING", "mode": "NULLABLE"},
            {"name": "topics", "type": "STRING", "mode": "NULLABLE"},
            {"name": "avg_views", "type": "FLOAT", "mode": "NULLABLE"},
        ]
    }
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read Entire JSON File" >> beam.Create([read_entire_file(options.input2)])
            | "Parse JSON Array" >> beam.ParDo(ParseJsonArray())
            | "Transform Records" >> beam.ParDo(TransformJsonRecord())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=options.output2,
                schema=table_schema,
                custom_gcs_temp_location="gs://tem12",
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        return "channel done"