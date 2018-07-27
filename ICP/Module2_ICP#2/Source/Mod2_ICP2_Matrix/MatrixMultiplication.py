import os

os.environ["SPARK_HOME"] = "/usr/local/spark/spark-2.3.1-bin-hadoop2.7"

from operator import add

from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext.getOrCreate()

    lines = sc.textFile("/Users/snehamishra/PycharmProjects/Mod2_ICP2_MergeSort/input.txt", 1)
    with open("/Users/snehamishra/PycharmProjects/Mod2_ICP2_MergeSort/input.txt") as f:
        listInput = f.read().split(' ')

    nums = sc.parallelize(listInput)

    counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).sortByKey(add)

    nums.saveAsTextFile("output3")
    counts.saveAsTextFile("output2")
    sc.stop()
