import clusters
from taxitrips import TaxiTrips
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


def start():
    spark = SparkSession.builder.appName("GeoSpatial Data Analysis").getOrCreate()
    coordinates_list = TaxiTrips(spark).get_coordinates_list()

    clusters.kmeans(coordinates_list, spark)

start()