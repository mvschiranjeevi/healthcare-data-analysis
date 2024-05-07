from datetime import datetime

from src.database import DatabaseConnector
from src.s3 import S3Connector


def consume_data(
        db_connector: DatabaseConnector,
        s3_connector: S3Connector,
        curated_bucket: str,
        report: str,
):
    try:
        print(f"Publish data to table {report}")
        s3_key = f"{report}/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"
        df = s3_connector.read_parquet_to_df(bucket=curated_bucket, key=s3_key)
        db_connector.write_to_db(data_frame=df, db_table=report, memory_partition=20)
        print("Publishing report data to data read store successfully.")
    except Exception as exp:
        print(f"Error when publishing data to data read database. Please check the Stack Trace. {exp}")
        raise
