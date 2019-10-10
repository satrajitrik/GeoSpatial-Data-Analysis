from pyspark.ml.feature import VectorAssembler


def kmeans(coordinates_list):
    coordinates_list = [
        [float(coordinates[0]), float(coordinates[1])]
        for coordinates in coordinates_list
    ]
    df = spark.createDataFrame(coordinates_list, ["Longitude", "Latitude"])

    vecAssembler = VectorAssembler(
        inputCols=["Longitude", "Latitude"], outputCol="features"
    )
    new_df = vecAssembler.transform(df)
    new_df.show()

    kmeans = KMeans(k=2, seed=1)  # 2 clusters here
    model = kmeans.fit(new_df.select("features"))

    transformed = model.transform(new_df)
    transformed.show()
