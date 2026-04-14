
CREATE EXTERNAL TABLE youtube_trending (
    video_id STRING,
    title STRING,
    channel STRING,
    category_id STRING,
    published_at STRING,
    views BIGINT,
    likes BIGINT
)
PARTITIONED BY (
    year INT,
    month INT,
    day INT
)
STORED AS PARQUET
LOCATION 's3://youtube-data-lake-abdallah-ashraf/cleansed/';