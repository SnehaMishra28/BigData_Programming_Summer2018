from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Load training data
from pyspark.python.pyspark.shell import spark

#data = spark.read.format("libsvm").load("/Users/snehamishra/Downloads/PySpark_MLib_ICP6/data/classification/iris_libsvm.txt")
data = spark.read.load("/Users/snehamishra/Downloads/Absenteeism_at_work_AAA/Absenteeism_at_work.csv", format="csv", header=True, delimeter=";")

#selecting the column to run the
data.select("Reason for absence", "Month of absence","Seasons","Absenteeism time in hours")

# Split the data into train and test
splits = data.randomSplit([0.6, 0.4])
#splits = data.randomSplit([0.6, 0.4], 1234)
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