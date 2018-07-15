import os

os.environ["SPARK_HOME"] = "/usr/local/Cellar/spark-2.3.1-bin-hadoop2.7"

from pyspark.sql import SparkSession
from pyspark import SparkContext


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *

print("\nRead Data from File")

football_df = spark.read.option("header", "true").csv("/Users/aditya/Downloads/lab1 csv/WorldCups.csv")


#Number of World Cups
count = football_df.count()


#Increase in the countries playing the sport
maxteams = football_df.select('QualifiedTeams').rdd.max()[0]
minteams = football_df.select('QualifiedTeams').rdd.min()[0]
increase = ((int(maxteams) - int(minteams))/int(minteams)) * 100

#print("The percentage increase in number of teams is " + str(increase) + "%")


# Maximum number of world cup wins:
#df.groupBy(['Winner']).count().orderBy("count", ascending=False).show(1)

# Most unluckiest Country in the history of World Cups:

#football_df.groupBy(['Football Runners-Up']).count().orderBy("count", ascending=False).show()

# Most number of times a country has hosted:

#football_df.groupBy(['Countries']).count().orderBy("count", ascending=False).show(1)

# Creating another dataset

print("\nRead Data from File")

cricket_df = spark.read.option("header", "true").csv("/Users/aditya/Downloads/lab1 csv/cric.csv")

#cricket_df.show()

unionDf = football_df.unionAll(cricket_df)

#unionDf.show()

#unionDf.write.option("header", "true").csv("Union_Output")

joinDf = football_df.join(cricket_df, ["Countries"])

#joinDf.write.option("header", "true").csv("Join_Output")
#joinDf.show()

#When the football winner and the cricket winner matched:

#football_df.join(cricket_df, football_df['Football Winner'] == cricket_df['Cricket Runner-Up']).show(1)

#Difference in years between the 1st football world cup and the first Cricket World Cup:

yeardiff = int(cricket_df.select('Cricket_Year').rdd.min()[0]) - int(football_df.select('Football_Year').rdd.min()[0])
#print(yeardiff)

# Number of distinct countries who have won the world cup:

distinctcount = football_df.select('Football Winner').distinct().count()

#print("\nIn " + str(count) + " World Cups; there have been " + str(distinctcount) + " different Winners")


###### RDD's and DataFrames ####

footballrdd = football_df.select(football_df.columns)
cricketrdd = cricket_df.select(cricket_df.columns)

# Join

# footballrdd.join(cricketrdd, "Countries").rdd.saveAsTextFile("joinop")

# Union

# footballrdd.unionAll(cricketrdd).rdd.saveAsTextFile("unionop")

# distinct

#footballrdd.select('Football Winner').distinct().rdd.saveAsTextFile("distinctop")

# Count

#print(footballrdd.count())

# Distinct Count

print(footballrdd.select('Football Winner').distinct().count())
/Users/snehamishra/PycharmProjects/BDP_mod2_Lab1