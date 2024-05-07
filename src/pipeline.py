from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.consume import consume_data
from src.database import DatabaseConnector
from src.ingest import ingest
from src.preprocessing import Preprocessing
from src.s3 import S3Connector
from src.transform import Transform


def create_spark_object(app_name: str):
    conf = SparkConf()
    conf.set("spark.jars.packages", "com.amazonaws:aws-java-sdk:1.11.469,org.apache.hadoop:hadoop-aws:3.3.1")
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.access.key", "foo")
    conf.set("spark.hadoop.fs.s3a.secret.key", "foo")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.driver.memory", "12g")
    conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    conf.set("spark.jars", "./postgresql-42.7.3.jar,./mysql-connector-j-8.4.0.jar")
    conf.set('spark.driver.extraClassPath', './postgresql-42.7.3.jar,./mysql-connector-j-8.4.0.jar')

    try:
        print(f"Creating spark session object with appname - {app_name} ...")
        spark = SparkSession.builder.config(conf=conf).master("local").appName(app_name).getOrCreate()
        print("Created spark session instance successfully.")
        return spark
    except NameError as exp:
        print(f"NameError in method - {__name__}(). Please check the Stack Trace. {exp}")
        raise exp
    except Exception as exp:
        print(f"Error in method - {__name__}(). Please check the Stack Trace. {exp}")
        raise exp


def main():
    print("Starting pipeline...")

    spark = create_spark_object("pipeline")

    s3_connector = S3Connector(spark)

    raw_bucket = "raw-bucket"
    cleaned_bucket = "cleaned-bucket"
    curated_bucket = "curated-bucket"

    source_db = DatabaseConnector(spark, "postgresql", "localhost", 5432, "postgres", "postgres", "postgres")
    report_db = DatabaseConnector(spark, "mysql", "localhost", 3306, "report", "root", "root")

    ingest(source_db, s3_connector, raw_bucket, "drug", numPartitions=20)
    ingest(source_db, s3_connector, raw_bucket, "prescriber", numPartitions=20)
    ingest(source_db, s3_connector, raw_bucket, "prescriber_drug", limit=1000000, numPartitions=20)

    preprocessing = Preprocessing(
        s3_connector=s3_connector,
        raw_bucket=raw_bucket,
        cleansed_bucket=cleaned_bucket,
    )

    preprocessing.clean_drug_data()
    preprocessing.clean_prescriber_data()
    preprocessing.clean_prescriber_drug_data()

    transform = Transform(
        s3_connector=s3_connector,
        cleansed_bucket=cleaned_bucket,
        curated_bucket=curated_bucket,
    )
    transform.drug_report()

    consume_data(
        db_connector=report_db,
        s3_connector=s3_connector,
        curated_bucket=curated_bucket,
        report="drug_report",
    )

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
