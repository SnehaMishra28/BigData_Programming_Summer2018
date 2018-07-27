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

sc = SparkContext(appName="PysparkStreaming")
ssc = StreamingContext(sc, 5)   #Streaming will execute in each 3 seconds

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 12345)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
counts = words.map(lambda word: (len(word), word))\
    .reduceByKey(lambda x, y: x + " " + y)

# Print the elements with the count of each RDD generated in this DStream to the console
counts.pprint()

ssc.start()
ssc.awaitTermination()
