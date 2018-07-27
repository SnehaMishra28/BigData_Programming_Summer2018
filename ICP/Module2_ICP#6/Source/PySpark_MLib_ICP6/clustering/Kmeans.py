from pyspark.ml.clustering import KMeans


# Loads data.
from pyspark.python.pyspark.shell import spark

dataset = spark.read.format("libsvm").load("/Users/rohithkumarn/Desktop/MachineLearning/data/clustering/sample_kmeans_data.txt")

# Trains a k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)