import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import sys
from utility import time_it


def generate_df_with_n_rows(rows):
    a = [[random.randint(1, 10)] for row in range(rows)]
    return spark.createDataFrame(a, StructType([StructField("a", StringType(), True)]))


@time_it
def inner_join(df_one, df_two, key_to_join_on):
    df_three = df_one.join(df_two, on=[key_to_join_on])
    df_three.collect()


@time_it
def left_join(df_one, df_two):
    df_three = df_one.join(df_two, df_one.a == df_two.a, "left")
    df_three.collect()


@time_it
def right_join(df_one, df_two):
    df_three = df_one.join(df_two, df_one.a == df_two.a, "right")
    df_three.collect()


def main():
    num_rows_to_generate = sys.argv[1]
    type_of_join = sys.argv[2]

    print(f"number of rows to generate {num_rows_to_generate} for join type {type_of_join}")
    df_one, df_two = generate_df_with_n_rows(int(num_rows_to_generate)), generate_df_with_n_rows(int(num_rows_to_generate))

    if type_of_join == "inner":
        inner_join(df_one, df_two, 'a')
    elif type_of_join == "left":
        left_join(df_one, df_two)
    elif type_of_join == "right":
        right_join(df_one, df_two)


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    main()

