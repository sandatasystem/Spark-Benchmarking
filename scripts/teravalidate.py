import sys
from time import time
from pyspark.sql import SparkSession


def verify_partition_is_sorted(split_index, partition):
    previous_key = None
    minimum_value = None
    for key_bytes, value_bytes in partition:
        key = int(''.join([str(key_bytes[index]) for index in range(len(key_bytes))]))

        if previous_key:
            assert key >= previous_key
        else:
            minimum_value = key

        previous_key = key

    yield split_index, previous_key, minimum_value


def main():
    rdd = SC.newAPIHadoopFile(
        f"file://{TERAVALIDATE_INPUT_DIR}",
        'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat',
        'org.apache.hadoop.io.Text',
        'org.apache.hadoop.io.Text'
    )

    rdd = rdd.mapPartitionsWithIndex(verify_partition_is_sorted, preservesPartitioning=True)
    final = []
    for partition, max_value, min_value in rdd.collect():
        final.append((partition, max_value, min_value))


if __name__ == '__main__':
    TERAVALIDATE_INPUT_DIR = sys.argv[1]
    SC = SparkSession.builder.appName("Spark Teragen").getOrCreate().sparkContext

    before_time = time()

    main()

    after_time = time()

    print(f"Dataset is correctly sorted")
    print(f"Operation took: {after_time - before_time} seconds")

