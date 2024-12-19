import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.filesystems import FileSystems
import json
from datetime import datetime
import isodate

class ParseJsonArray(beam.DoFn):
    def process(self, element):
        """Parse and process JSON array."""
        try:
            records = json.loads(element)
            if not isinstance(records, list):
                raise ValueError("Expected a JSON array, got something else")
            for record in records:
                yield record
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing JSON array: {e}")
            return

class TransformVideoAnalyticsRecord(beam.DoFn):
    def process(self, record):
        """Transform each YouTube video analytics record."""
        try:
            # Extract and format date
            published_date = datetime.strptime(record.get("published_date", "1970-01-01T00:00:00Z"), "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d")
            published_datetime = datetime.strptime(published_date, "%Y-%m-%d")
            year = published_datetime.year
            month = published_datetime.month
            day = published_datetime.day
            weekday = published_datetime.strftime("%A")
            # Parse duration to seconds
            duration = isodate.parse_duration(record.get("duration", "PT0S")).total_seconds()
            # Calculate metrics
            views = int(record.get("views", 0))
            likes = int(record.get("likes", 0))
            comments = int(record.get("comments", 0))
            metrics = ((likes + comments) / views) * 100 if views > 0 else 0
            # Check if viral
            isviral = "viral" if views >= 5000000 else "Not Viral"
            # Process tags
            tags = record.get("tags", [])
            tags = ",".join(tags) if isinstance(tags, list) else str(tags)
            # Yield the transformed record
            yield {
                "channel_name": record.get("channel_name", "Unknown"),
                "video_id": record.get("video_id"),
                "title": record.get("title", "Unknown"),
                "views": views,
                "likes": likes,
                "comments": comments,
                "category_id": record.get("category_id", "Unknown"),
                "tags": tags,
                "description": record.get("description", "No Description").strip(),
                "published_date": published_date,
                "duration": duration,
                "year": year,
                "month": month,
                "day": day,
                "weekday": weekday,
                "metrics": metrics,
                "isviral": isviral,
            }
        except Exception as e:
            print(f"Error processing record: {record} -> {e}")
            return

def read_entire_file(file_path):
    """Read the entire content of a JSON file from GCS."""
    with FileSystems.open(file_path) as f:
        return f.read().decode("utf-8")

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input1', help='Input file path on GCS', default="gs://sidemen-buckets1/youtube/video_analytics.json")
        parser.add_argument('--output1', help='Output BigQuery table', default="youtube-analytics-444717:youtube_datasets1.video_stats")

def run_video_pipeline():
    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(MyOptions)
    table_schema = {
        "fields": [
            {"name": "channel_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "video_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "views", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "likes", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "comments", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "category_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tags", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "published_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "duration", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "year", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "month", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "day", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "weekday", "type": "STRING", "mode": "NULLABLE"},
            {"name": "metrics", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "isviral", "type": "STRING", "mode": "NULLABLE"},
        ]
    }
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read Entire JSON File" >> beam.Create([read_entire_file(options.input1)])
            | "Parse JSON Array" >> beam.ParDo(ParseJsonArray())
            | "Transform Records" >> beam.ParDo(TransformVideoAnalyticsRecord())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=options.output1,
                schema=table_schema,
                custom_gcs_temp_location="gs://tem12",
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        return "video done"


