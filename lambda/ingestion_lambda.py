import os
import boto3
import json
from datetime import datetime
import urllib.request

s3 = boto3.client('s3')
glue = boto3.client('glue')
cloudwatch = boto3.client('cloudwatch')

BUCKET_NAME = os.environ.get("BUCKET_NAME")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME")  

def push_metric(name, value):
    cloudwatch.put_metric_data(
        Namespace='YouTubePipeline',
        MetricData=[
            {
                'MetricName': name,
                'Value': value,
                'Unit': 'Count'
            }
        ]
    )

def lambda_handler(event, context):
    try:
        # 1. CALL YOUTUBE API
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode=US&maxResults=50&key={YOUTUBE_API_KEY}"
        
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())

        # 2. SAVE RAW DATA TO S3
        now = datetime.now()
        filename = f"raw/year={now.year}/month={now.month}/day={now.day}/{now.strftime('%Y-%m-%d_%H-%M-%S')}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data)
        )

        record_count = len(data.get("items", []))

        # 3. CLOUDWATCH METRIC - SUCCESS + DATA VOLUME
        push_metric("PipelineSuccess", 1)
        push_metric("VideosFetched", record_count)

        # 4. TRIGGER GLUE JOB 🚀
        glue_response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--input_path": f"s3://{BUCKET_NAME}/{filename}"
            }
        )

        return {
            "status": "success",
            "file": filename,
            "records": record_count,
            "glue_job_run_id": glue_response["JobRunId"]
        }

    except Exception as e:
        # 5. CLOUDWATCH FAILURE METRIC
        push_metric("PipelineFailure", 1)

        return {
            "status": "error",
            "message": str(e)
        }