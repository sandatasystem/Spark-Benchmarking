import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import time
from utility import time_it
from argparse import ArgumentParser


def generate_df_with_n_rows(rows):
    return spark.createDataFrame(
        [
            [random.randint(1, 10)] for row in range(rows)
        ],
        StructType([StructField("a", StringType(), True)])
    )


@time_it
def perform_inner_join(df_one, df_two, key_to_join_on):
    df_one.join(df_two, on=[key_to_join_on]).show()


def configure_argparse():
    parser = ArgumentParser()
    parser.add_argument("num_rows_to_generate", type=str, help="Number of rows to produce for each dataframe", nargs='+')
    args = parser.parse_args()
    return ' '.join(args.num_rows_to_generate)


def main():
    num_rows_to_generate = configure_argparse()
    table = []
    for num_rows in num_rows_to_generate.split(" "):
        print(f"GENERATING DATAFRAME WITH {num_rows} ROWS")
        df_one, df_two = generate_df_with_n_rows(int(num_rows)), generate_df_with_n_rows(int(num_rows))

        # LETS TIME THE FOLLOWING OPERATIONS
        perform_inner_join(df_one, df_two, 'a')


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Benchmark").getOrCreate()
    main()

