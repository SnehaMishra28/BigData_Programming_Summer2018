from pyspark.sql import SparkSession

import pandas as pd


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *


#print("\nRead Data from ConsumerComplaints File")
df = spark.read.option("header","true").csv("/Users/snehamishra/Downloads/ConsumerComplaints.csv")
#df = spark.read.option("header","true").csv("/Users/snehamishra/Downloads/201508_trip_data.csv")

#print("\nPrinting Schema of Stored Data")
df.printSchema()

# DataFrames can be saved as Parquet files, maintaining the schema information.
#df.write.parquet("ConsumerComplaints.parquet")

# Read in the Parquet file created above.
# Parquet files are self-describing so the schema is preserved.
# The result of loading a parquet file is also a DataFrame.
#parquetFile = spark.read.parquet("ConsumerComplaints.parquet")

# Print the distinct values only
print("\n Print the duplicate count and remove unique count:")
uniqueCount = df.count() - df.distinct().count()
print("\nrepeated count - "+str(uniqueCount))

#Create second dataframes for union set data values to df2
data1 = {'Name':['Sneha', 'Aditya', 'Plam', 'Swati'],'Age':[28,34,29,42]}
#df2 = pd.DataFrame(data1)
#df1 = spark.createDataFrame(data1)
#df1.show()

df1 = spark.read.option("header","true").csv("/Users/snehamishra/Desktop/myComplaints .csv")
#union of df and df1
df3 = df.unionAll(df1)
#df3.write.csv("/Users/snehamishra/Desktop/complaintsUnion")
df3.show()

df3.orderBy(df3['Company'].desc()).show()
#df3.write.csv("/Users/snehamishra/Desktop/AlphabeticallyDesc")

df3.groupBy('Zip Code').count().show()

df.crossJoin(df1).show()

df3.groupBy('Zip Code').avg().show()

thirteen = df3.take(13)[-1]
print(thirteen)
#df4 = spark.createDataFrame([thirteen])
#df4.show()
