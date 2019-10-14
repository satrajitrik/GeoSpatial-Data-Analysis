import clusters
from taxitrips import TaxiTrips
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("GeoSpatial Data Analysis").getOrCreate()
    coordinates_list = TaxiTrips(spark).get_coordinates_list()

    predictions = clusters.kmeans(coordinates_list, spark)


if __name__ == "__main__":
    main()
