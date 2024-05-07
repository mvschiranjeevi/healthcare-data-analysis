from datetime import datetime

from src.s3 import S3Connector
from pyspark.sql import functions as func

from src.validation import count_rows, top_rows, print_schema


class Transform:

    def __init__(self, s3_connector: S3Connector, cleansed_bucket: str, curated_bucket: str):
        self.s3_connector = s3_connector
        self.cleansed_bucket = cleansed_bucket
        self.curated_bucket = curated_bucket
        self.s3_key = f"year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}"

    def drug_report(self):

        try:
            print("Reading cleansed drug and prescriber_drug data...")
            s3_presc_drug_key = f"prescriber_drug/{self.s3_key}"

            s3_drug_key = f"drug/{self.s3_key}"
            prescriber_drug_df = self.s3_connector.read_parquet_to_df(bucket=self.cleansed_bucket,
                                                                      key=s3_presc_drug_key)
            drug_df = self.s3_connector.read_parquet_to_df(bucket=self.cleansed_bucket, key=s3_drug_key)

            print("Merging prescriber_drug and drug data for drug report...")
            prescriber_drug_df = prescriber_drug_df.select(
                "drug_brand_name",
                "drug",
                "total_claims",
                func.col("total_drug_cost").alias("total_cost"),
            )

            prescriber_drug_df = prescriber_drug_df.groupBy("drug_brand_name", "drug").agg(
                func.sum("total_claims").alias("total_claims"),
                func.sum("total_cost").alias("total_cost"),
            )

            drug_df = drug_df.alias("drug")
            prescriber_drug_df = prescriber_drug_df.alias("prescriber_drug")

            drug_report_df = prescriber_drug_df.join(
                drug_df,
                (prescriber_drug_df["drug_brand_name"] == drug_df["drug_brand_name"])
                & (prescriber_drug_df["drug"] == drug_df["drug"])
            ).select("prescriber_drug.*", "drug.drug_type")

            print("Validating drug curated data...")
            count_rows(drug_report_df)

            top_rows(drug_report_df)

            print_schema(drug_report_df)

            print("Writing drug report to curated zone")

            drug_report_df = (
                drug_report_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )

            self.s3_connector.write_df_to_parquet(
                bucket=self.curated_bucket,
                key="drug_report",
                data_frame=drug_report_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite"
            )

            print("Drug report data written to curated zone successfully.")

        except Exception as exp:
            print(f"Error in method - drug_report(). Please check the Stack Trace. {exp}")
            raise exp

    def prescriber_report(self):
        try:
            print("Reading cleansed prescriber data...")
            s3_prescriber_key = f"prescriber/{self.s3_key}"
            prescriber_df = self.s3_connector.read_parquet_to_df(bucket=self.cleansed_bucket, key=s3_prescriber_key)
            prescriber_df = prescriber_df.select(
                "npi",
                "nppes_provider_last_org_name",
                "nppes_provider_first_name",
                "nppes_provider_city",
                "nppes_provider_state",
                "nppes_provider_country",
                "specialty_description",
                "description_flag",
                "drug_name",
                "generic_name",
                "bene_count",
                "total_claim_count",
                "total_day_supply",
                "total_drug_cost",
                "bene_count_ge65",
                "bene_count_ge65_suppress_flag",
                "total_claim_count_ge65",
                "ge65_suppress_flag",
                "total_day_supply_ge65",
                "total_drug_cost_ge65",
            )

            print("Validating prescriber data")
            count_rows(prescriber_df)

            top_rows(prescriber_df)

            print_schema(prescriber_df)

            print("Writing prescriber data to curated zone")

            prescriber_df = (
                prescriber_df.withColumn("year", func.lit(datetime.now().year))
                .withColumn("month", func.lit(datetime.now().month))
                .withColumn("day", func.lit(datetime.now().day))
            )

            self.s3_connector.write_df_to_parquet(
                bucket=self.curated_bucket,
                key="drug_report",
                data_frame=prescriber_df,
                disk_partition_col=["year", "month", "day"],
                memory_partition=20,
                write_mode="overwrite"
            )

            print("Prescriber report data written to curated zone successfully.")

        except Exception as exp:
            print(f"Error in method - prescriber_report(). Please check the Stack Trace. {exp}")
            raise exp
