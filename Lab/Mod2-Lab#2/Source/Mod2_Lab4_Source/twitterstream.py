from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql.functions import desc

from collections import namedtuple

import os

os.environ["SPARK_HOME"] = "/usr/local/spark/spark-2.3.1-bin-hadoop2.7/"


def main():
    sc = SparkContext(appName="PysparkStreaming")
    wordcount = {}
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream("localhost", 5555)

    fields = ("word", "count")
    Tweet = namedtuple('Text', fields)

    # lines = socket_stream.window(20)
    counts = lines.flatMap(lambda text: text.split(" "))\
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: a + b).map(lambda rec: Tweet(rec[0], rec[1]))

    #counts = lines. \
     #   flatMap(lambda line: line.split(" ")) \
      #  .groupByKey() \
       ##.reduceByKey(lambda a, b: a + b)

    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
