import os
os.environ["PYSPARK_SUBMIT_ARGS"] = ("pyspark --packages graphframes:graphframes:0.5.0-spark2.0-s_2.11")

from graphframes import *

from pyspark.sql import SparkSession


