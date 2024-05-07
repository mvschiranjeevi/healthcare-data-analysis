from pyspark.sql import SparkSession, DataFrame


class DatabaseConnector:
    def __init__(self, spark: SparkSession, rdbms: str, host: str, port: int, db_name: str, user: str, password: str):
        self.spark = spark
        self._url = f"jdbc:{rdbms}://{host}:{port}/{db_name}"

    def write_to_db(self, data_frame: DataFrame, db_table, memory_partition=None, write_mode="overwrite", **kwargs):
        try:
            print("Start writing dataframe into database...")
            if memory_partition:
                current_partition_num = data_frame.rdd.getNumPartitions()
                if current_partition_num > memory_partition:
                    data_frame = data_frame.coalesce(memory_partition)
                elif current_partition_num < memory_partition:
                    data_frame = data_frame.repartition(memory_partition)
            (data_frame.write.format("jdbc").mode(write_mode).option("url", self._url)
             .option("dbtable", db_table)
             .option("user", "root")
             .option("password", "root")
             .option("driver", "com.mysql.jdbc.Driver")
             .save())

            print("Write dataframe to database successfully.")

        except Exception as exp:
            print(f"Error when writing to database. Please check the Stack Trace. {exp}")
            raise exp

    def read_table_to_df(self, db_table, **kwargs):
        try:
            print(f"Reading {db_table} from database...")
            data_frame = (self.spark.read.format("jdbc").option("url", self._url).option("dbtable", db_table)
                          .option("user", "postgres").option("password", "postgres")
                          .option("driver", "org.postgresql.Driver").load())

            print(f"Read {db_table} from database successfully.")
            return data_frame
        except Exception as exp:
            print(f"Error when reading from database. Please check the Stack Trace. {exp}")
            raise exp
