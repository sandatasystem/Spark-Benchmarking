from pyspark.sql import SparkSession
import random
import sys
import os


def generate():
    """
    For each partition, generate a file
    """

    output_file_format = 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'

    rdd_ = SC.parallelize(
        [partition_index for partition_index in range(NUMBER_OF_PARTITIONS)]
    ).mapPartitionsWithIndex(generate_rows_for_partition)

    rdd_.collect()

    rdd_.saveAsNewAPIHadoopFile(f'file://{TERAGEN_OUTPUT_DIR}', output_file_format)


def generate_rows_for_partition(partition_number, value):
    """
    For each partition generate rows
    :param partition_number:
    :param value:
    :return:
    """
    first_record_number = partition_number * NUMBER_OF_ROWS_PER_PARTITION

    rows = []
    for record_number in range(first_record_number,
                               first_record_number + NUMBER_OF_ROWS_PER_PARTITION):
        row = generate_single_row(record_number)

        # Creating pair rdd of form (KEY, VALUE) where key is first 10 bits of row, value is
        # remaining bits
        rows.append((row[:10], row[10:]))

    return rows


def generate_single_row(row_id):
    """
    This will produce a single row with 100 bytes composed of the following:

    <10 bytes random key> <10 bytes of the row ID> <78 bytes of random data><newline character>

    :param row_id:
    :return:
    """
    list_of_bytes = []

    # First 10 bytes of random ints
    list_of_bytes.extend([random.randint(0, 255) for iteration in range(10)])

    right_justified_row_id = str(row_id).rjust(10, '0')

    # Next 10 bytes will be based off row id
    list_of_bytes.extend([int(right_justified_row_id[index]) for index in range(len(right_justified_row_id))])

    # 78 bytes of random data
    list_of_bytes.extend([random.randint(0, 255) for iteration in range(78)])

    print(len(list_of_bytes))

    return bytes(list_of_bytes)


def get_size_of_data_generated():
    total_size = 0
    for generated_file in os.listdir(TERAGEN_OUTPUT_DIR):
        if "part-" in generated_file:
            total_size += os.path.getsize(f"{TERAGEN_OUTPUT_DIR}/{generated_file}")

    print(f"TOTAL SIZE IS: {total_size}, BYTES EXPECTED SIZE IS {NUMBER_OF_ROWS_TO_GENERATE * 100}")


if __name__ == '__main__':
    SC = SparkSession.builder.appName("Spark Teragen").getOrCreate().sparkContext

    NUMBER_OF_ROWS_TO_GENERATE = int(sys.argv[1])
    TERAGEN_OUTPUT_DIR = sys.argv[2]
    NUMBER_OF_PARTITIONS = SC.defaultParallelism

    if len(sys.argv) > 3:
        NUMBER_OF_PARTITIONS = int(sys.argv[3])

    NUMBER_OF_ROWS_PER_PARTITION = NUMBER_OF_ROWS_TO_GENERATE // NUMBER_OF_PARTITIONS

    generate()
    get_size_of_data_generated()

