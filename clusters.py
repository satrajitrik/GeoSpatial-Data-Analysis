from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator


def kmeans(coordinates_list, spark):
    coordinates_list = [
        [float(coordinates[0]), float(coordinates[1])]
        for coordinates in coordinates_list
    ]
    df = spark.createDataFrame(coordinates_list, ["Longitude", "Latitude"])

    vecAssembler = VectorAssembler(
        inputCols=["Longitude", "Latitude"], outputCol="features"
    )
    new_df = vecAssembler.transform(df)

    silhouettes = []
    for k in range(2, 10):
        kmeans = KMeans().setK(k).setSeed(1)
        model = kmeans.fit(new_df.select("features"))
        predictions = model.transform(new_df)

        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(predictions)
        silhouettes.append([silhouette, predictions, k])

    _, predictions, k = min(silhouettes, key=lambda x: x[0])

    predictions.show()

    return predictions
