import boto3
import os

class S3Connector:

    def __init__(self) -> None:
        self.s3 = boto3.resource(
            service_name = 's3',
            region_name = 'us-east-1',
            aws_access_key_id = os.getenv("AWS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )


    def send(self):
        """Sends the csv file to an S3 bucket in AWS
        """
        bucket = 'jake-ufc-project'
        path_raw = "/home/whale/Documents/Coding/ufc/Ufc-Web-Scraper/data/raw_data.csv"

        self.s3.meta.client.upload_file(path_raw, bucket, "raw_data/raw_data.csv")



conn = S3Connector()
conn.send()