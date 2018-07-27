import os

from pyspark.ml.feature import VectorAssembler

#os.environ['SPARK_HOME'] = "/Users/aditya/PycharmProjects/ICP6/venv/lib/python3.6/site-packages/pyspark"

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Load training data
from pyspark.ml.linalg import SparseVector
# from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = spark.read.load("/Users/snehamishra/Downloads/Absenteeism_at_work_AAA/Absenteeism_at_work.csv", format="csv", header=True, delimiter=";")
data = data.withColumn("MOA", data["Month of absence"] - 0).withColumn("label", data['Seasons'] - 0)
data.show()
assem = VectorAssembler(inputCols=["MOA"], outputCol='features')
data = assem.transform(data)
# Split the data into train and test
splits = data.randomSplit([0.6, 0.4], 1234)
train = splits[0]
test = splits[1]

# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")


# train the model
model = nb.fit(train)

# select example rows to display.
predictions = model.transform(test)
predictions.show()


# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))
