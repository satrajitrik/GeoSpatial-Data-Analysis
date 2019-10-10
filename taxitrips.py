from ast import literal_eval as make_tuple


class TaxiTrips(object):
    def __init__(self, spark):
        self.df = (
            spark.read.format("com.databricks.spark.csv")
            .options(header="true", delimiter=";", inferschema="true")
            .load("dbfs:/FileStore/tables/yellow_trip_sample_100000.csv")
        )

    def show_data(self):
        display(df)

    def get_coordinates_list(self):
        return [
            make_tuple(row.PickUpCoordinates)
            for row in self.df.select("PickUpCoordinates").collect()
        ]
