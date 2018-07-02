import os
import numpy

os.environ["SPARK_HOME"] = "/usr/local/spark/spark-2.3.1-bin-hadoop2.7/"

from operator import add

from pyspark import SparkContext

if __name__ == "main":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("/Users/snehamishra/Downloads/Spark_Python_Code/matrixInput.txt", 1)

    # Split line into array of entry data
    entry = lines.split(",")
    # Set row, column, and value for this entry
    row = int(entry[1])
    col = int(entry[2])
    value = float(entry[3])

    # If this is an entry in matrix A...
    if entry[0] == "A":

        # Generate the necessary key-value pairs
        for i in range(col):
                print('<{}{},{} {} {}}>'.format(row, i, "A", col, value))
    # Otherwise, if this is an entry in matrix B...
    else:
        # Generate the necessary key-value pairs
        for i in range(row):
                print('<{}{},{} {} {}}>'.format(i, col, "B", row, value))