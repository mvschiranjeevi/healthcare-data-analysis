from datetime import datetime

from src.s3 import S3Connector
from pyspark.sql import functions as func

from src.validation import count_rows, top_rows, print_schema


class Preprocessing:

    def __init__(self, s3_connector: S3Connector, raw_bucket: str, cleansed_bucket: str):
        """
        Constructor for class Preprocessing
        """
        self._s3_connector = s3_connector
        self._raw_bucket = raw_bucket
        self._cleansed_bucket = cleansed_bucket

        self.s3_key = f"year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"

    def clean_drug_data(self):
        try:
            print("Cleaning drug data")
            s3_drug_key = f"drug/{self.s3_key}"
            drug_df = self._s3_connector.read_parquet_to_df(bucket=self._raw_bucket, key=s3_drug_key)

            drug_df = drug_df.select(
                func.col("brnd_name").alias("drug_brand_name"),
                func.col("gnrc_name").alias("drug"),
                func.col("antbtc_drug_flag").alias("is_antibiotic"),
            )

            drug_df = drug_df.withColumn(
                "drug_type",
                func.when(func.col("is_antibiotic") == "Y", "Antibiotic").otherwise(
                    "Non-antibiotic"
                ),
            ).drop("is_antibiotic")

            drug_df = drug_df.groupBy("drug_brand_name", "drug").agg(
                func.last("drug_type").alias("drug_type"))

            print("Validating drug data")
            count_rows(drug_df)

            top_rows(drug_df)

            print_schema(drug_df)

            print("Writing drug data to cleansed zone")

            drug_df = (
                drug_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )

            self._s3_connector.write_df_to_parquet(
                bucket=self._cleansed_bucket,
                key="drug",
                data_frame=drug_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )

            print("Drug data cleaning completed")

        except Exception as e:
            print(f"Error when cleaning drug data. Please check the Stack Trace. {e}")
            raise

    def clean_prescriber_data(self):
        try:
            print("Cleaning prescriber data")
            s3_prescriber_key = f"prescriber/{self.s3_key}"

            prescriber_df = self._s3_connector.read_parquet_to_df(bucket=self._raw_bucket, key=s3_prescriber_key)

            prescriber_df = prescriber_df.select(
                func.col("prscrbr_npi").alias("presc_id"),
                func.col("prscrbr_ent_cd").alias("presc_type"),
                func.col("prscrbr_city").alias("presc_city"),
                func.col("prscrbr_state_abrvtn").alias("prscrbr_state_code"),
            )

            prescriber_df = prescriber_df.withColumn(
                "presc_type",
                func.when(func.col("presc_type") == "I", "Individual").otherwise(
                    "Organization"))

            print("Validating prescriber data")
            count_rows(prescriber_df)

            top_rows(prescriber_df)

            print_schema(prescriber_df)

            print("Writing prescriber data to cleansed zone")

            prescriber_df = (
                prescriber_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )

            self._s3_connector.write_df_to_parquet(
                bucket=self._cleansed_bucket,
                key="prescriber",
                data_frame=prescriber_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )

            print("Prescriber data cleaning completed")

        except Exception as e:
            print(f"Error when cleaning prescriber data. Please check the Stack Trace. {e}")
            raise

    def clean_prescriber_drug_data(self):
        try:
            print("Cleaning prescriber drug data")
            s3_prescriber_drug_key = f"prescriber_drug/{self.s3_key}"

            prescriber_drug_df = self._s3_connector.read_parquet_to_df(bucket=self._raw_bucket,
                                                                       key=s3_prescriber_drug_key)

            prescriber_drug_df = prescriber_drug_df.select(
                func.col("prscrbr_npi").alias("presc_id"),
                func.col("prscrbr_last_org_name").alias("presc_lname"),
                func.col("prscrbr_first_name").alias("presc_fname"),
                func.col("prscrbr_state_abrvtn").alias("presc_state_code"),
                func.col("prscrbr_type").alias("presc_specialty"),
                func.col("brnd_name").alias("drug_brand_name"),
                func.col("gnrc_name").alias("drug"),
                func.col("tot_clms").alias("total_claims"),
                func.col("tot_drug_cst").alias("total_drug_cost"),
            )

            prescriber_drug_df = prescriber_drug_df.dropna(subset=["presc_specialty"])
            prescriber_drug_df = prescriber_drug_df.withColumn(
                "presc_fullname", func.concat_ws(" ", "presc_fname", "presc_lname")
            ).drop("presc_fname", "presc_lname")

            print("Validating prescriber drug data")
            count_rows(prescriber_drug_df)

            top_rows(prescriber_drug_df)

            print_schema(prescriber_drug_df)

            print("Writing prescriber drug data to cleansed zone")

            prescriber_drug_df = (
                prescriber_drug_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )

            self._s3_connector.write_df_to_parquet(
                bucket=self._cleansed_bucket,
                key="prescriber_drug",
                data_frame=prescriber_drug_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite",
            )

            print("Prescriber drug data cleaning completed")

        except Exception as e:
            print(f"Error when cleaning prescriber drug data. Please check the Stack Trace. {e}")
            raise
