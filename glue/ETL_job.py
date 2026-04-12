import sys
import boto3
import json
import pandas as pd
from datetime import datetime
from awsglue.utils import getResolvedOptions

s3 = boto3.client('s3')

# 1. Get argument from Lambda
args = getResolvedOptions(sys.argv, ['input_path'])
input_path = args['input_path']

# 2. Parse bucket + key from s3 path
bucket = input_path.split("/")[2]
key = "/".join(input_path.split("/")[3:])

# 3. Read JSON from S3
obj = s3.get_object(Bucket=bucket, Key=key)
data = json.loads(obj['Body'].read())

# 3. Extract features
records = []
for item in data['items']:
    records.append({
        "video_id": item.get("id"),
        "title": item['snippet'].get("title"),
        "channel": item['snippet'].get("channelTitle"),
        "category_id": item['snippet'].get("categoryId"),
        "published_at": item['snippet'].get("publishedAt"),
        "views": int(item['statistics'].get("viewCount", 0)),
        "likes": int(item['statistics'].get("likeCount", 0))
    })

df = pd.DataFrame(records)

now = datetime.now()
output_path = f"s3://youtube-data-lake-abdallah-ashraf/cleansed/year={now.year}/month={now.month}/day={now.day}/{now.strftime('%Y-%m-%d_%H-%M-%S')}.parquet"

df.to_parquet(output_path, index=False)

