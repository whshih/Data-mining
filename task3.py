import json
import sys
from pyspark import SparkContext


def emit_kV(line):
    lines = line.splitlines()
    data = json.loads(lines[0])
    output_tuple = (data['business_id'], 1)
    return output_tuple


def main():
    file_review = sys.argv[1]
    customized_or_not = sys.argv[3]
    n_partition = int(sys.argv[4])
    d = int(sys.argv[5])
    sc = SparkContext("local[*]", "task3")
    textRDD = sc.textFile(file_review).map(emit_kV)
    # Compute the business_IDs whose number over "n".
    if customized_or_not != "default":
        textRDD = textRDD.partitionBy(n_partition, hash)

    default = {}
    # Returns the number of partitions in RDD
    default["n_partition"] = textRDD.getNumPartitions()
    # get length of each partition
    default["n_items"] = textRDD.glom().map(len).collect()
    # Defult function
    count = textRDD.reduceByKey(
        lambda a, b: a + b).filter(lambda n: n[1] > d).collect()

    default["result"] = count
    with open(sys.argv[2], "a") as fp_out:
        json.dump(default, fp_out)


if __name__ == "__main__":
    main()
