from pyspark.sql.dataframe import DataFrame


def count_rows(df: DataFrame):
    try:
        row_count = df.count()
        print(f"Dataframe contains {row_count} rows.")
    except Exception as e:
        print(f"Error when validating number of rows. Please check the Stack Trace. {e}")
        raise


def top_rows(df: DataFrame, num_rows: int = 10):
    try:
        print(f"Dataframe's first {num_rows} rows are:")
        df_pandas = df.limit(num_rows).toPandas()
        print("\n\t" + df_pandas.__str__() + "\n")
    except Exception as e:
        print(f"Error when printing first {num_rows} rows. Please check the Stack Trace. {e}")
        raise


def print_schema(df: DataFrame):
    try:
        for field in df.schema.fields:
            print(f"\t\t\t\t {str(field)}")
    except Exception as e:
        print(f"Error when printing schema. Please check the Stack Trace. {e}")
        raise
