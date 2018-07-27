import sys
import os

os.environ["SPARK_HOME"] = "/usr/local/spark/spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="/Users/snehamishra/Documents/bigdata/hadoop-2.8.1"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""


def main():
    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 5)   #Streaming will execute in each 3 seconds
    #lines = ssc.textFileStream('log')  #'log/ mean directory name
    lines = ssc.socketTextStream("localhost", 12345)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
