from pyspark.sql import SparkSession


class S3Connector:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.spark_context = spark.sparkContext

    def check_object_count(self, bucket: str, key: str):
        s3_path = f"s3a://{bucket}"
        try:
            print(f"Retrieving number of objects in S3 path {s3_path} ...")
            objects = self.spark_context.wholeTextFiles(s3_path + "/").keys().collect()
            num_objects = len(objects)
            return num_objects
        except Exception as exp:
            print(f"Error when retrieving number of objects. Please check the Stack Trace. {exp}")
            raise exp

    def read_parquet_to_df(self, bucket: str, key: str, **kwargs):
        s3_path = f"s3a://{bucket}/{key}"
        try:
            print(f"Reading from {s3_path}...")
            data_frame = self.spark.read.options(**kwargs).parquet(s3_path)
            print(f"Read {key} from S3 successfully.")
            return data_frame
        except Exception as exp:
            print(f"Error when reading Parquet from S3. Please check the Stack Trace. {exp}")
            raise exp

    def write_df_to_parquet(self, bucket: str, key: str, data_frame, memory_partition=None, disk_partition_col=None,
                            **kwargs):
        try:
            print("Start writing dataframe into S3...")
            if memory_partition:
                current_partition_num = data_frame.rdd.getNumPartitions()
                print("Number of partitions: ", current_partition_num)
                if current_partition_num > memory_partition:
                    data_frame = data_frame.coalesce(memory_partition)
                elif current_partition_num < memory_partition:
                    data_frame = data_frame.repartition(memory_partition)
            s3_path = f"s3a://{bucket}/{key}"
            data_frame.write.partitionBy(*disk_partition_col).mode(kwargs["write_mode"]).parquet(s3_path)
            print("Write dataframe to S3 successfully.")
        except Exception as exp:
            print(f"Error when writing dataframe to S3. Please check the Stack Trace. {exp}")
            raise exp
