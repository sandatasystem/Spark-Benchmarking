from time import time
from pyspark.sql import SparkSession
import sys
import os


def regenerate_bytes_for_each_partition(row):
    key_bytes = []

    for key_str in str(row[0]):
        key_bytes.append(int(key_str))

    value_bytes = []

    for value_str in str(row[1]):
        value_bytes.append(int(value_str))

    return bytes(key_bytes), bytes(value_bytes)


def get_size_of_data_generated():
    total_size = 0
    for generated_file in os.listdir(TERASORT_OUTPUT_DIR):
        if "part-" in generated_file:
            total_size += os.path.getsize(f"{TERASORT_OUTPUT_DIR}/{generated_file}")

    print(f"TOTAL SIZE IS: {total_size}")


def main():
    output_file_format = 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
    input_file_format = 'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
    rdd = SC.newAPIHadoopFile(
        f'file://{TERAGEN_INPUT_DIR}',
        input_file_format,
        'org.apache.hadoop.io.Text',
        'org.apache.hadoop.io.Text'
    )

    rdd = rdd.map(lambda row: (int(''.join([str(row[0][index]) for index in range(10)])),
                               int(''.join([str(row[1][index]) for index in range(88)])))).\
        repartitionAndSortWithinPartitions(partitionFunc=lambda key: key // RANGE_PER_PARTITION).\
        map(regenerate_bytes_for_each_partition)

    rdd.saveAsNewAPIHadoopFile(f'file://{TERASORT_OUTPUT_DIR}', output_file_format)

    get_size_of_data_generated()


if __name__ == '__main__':
    MAX_KEY_VALUE = int(f"{9}{9}{9}{9}{9}{9}{9}{9}{9}{9}")
    TERAGEN_INPUT_DIR = sys.argv[1]
    TERASORT_OUTPUT_DIR = sys.argv[2]

    SC = SparkSession.builder.appName("Spark Terasort").getOrCreate().sparkContext
    RANGE_PER_PARTITION = MAX_KEY_VALUE // SC.defaultParallelism

    before_time = time()

    main()

    after_time = time()

    print(f"Operation took: {after_time - before_time} seconds")

