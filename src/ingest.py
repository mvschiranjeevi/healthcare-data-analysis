from datetime import datetime

import pyspark.sql.functions as func


def ingest(source_db_connector, s3_connector, raw_bucket, db_table, limit=None, **kwargs):
    try:
        table_query = None
        object_count = s3_connector.check_object_count(bucket=raw_bucket, key=db_table)
        print("Object count: ", object_count)
        if object_count == 0:
            print(f"Execute full load for table {db_table}")
            table_query = f"(SELECT * FROM {db_table}  {'LIMIT ' + str(limit) if limit else ''}) tmp"
        elif object_count > 0:
            df = s3_connector.read_parquet_to_df(bucket=raw_bucket, key=db_table)
            largest_id = df.agg(func.max("id")).collect()[0]
            print(f"Execute incremental load with current max id in datalake is {str(largest_id)}")
            table_query = f"(SELECT * FROM {db_table} WHERE id > {largest_id}) tmp"
        print(f"Table query: {table_query}")
        ingest_df = source_db_connector.read_table_to_df(db_table=table_query, **kwargs)
        ingest_df = (
            ingest_df.withColumn("year", func.lit(datetime.now().year))
            .withColumn("month", func.lit(datetime.now().month))
            .withColumn("day", func.lit(datetime.now().day))
        )

        s3_connector.write_df_to_parquet(bucket=raw_bucket, key=db_table, data_frame=ingest_df, memory_partition=20,
                                         disk_partition_col=["year", "month", "day"], write_mode="overwrite")
        del ingest_df
        print("Ingest data successfully.")

    except Exception as exp:
        print(f"Error in method - ingest_data(). Please check the Stack Trace. {exp}")
        raise exp
