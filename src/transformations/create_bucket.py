import boto3
s3 = boto3.client("s3", endpoint_url="http://university-minio:9000",
                  aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
                  use_ssl=False)
s3.create_bucket(Bucket="hudi-curated")
print("Bucket hudi-curated created")
