from ast import literal_eval as make_tuple
from config import Config


# Path = dbfs:/FileStore/tables/yellow_trip_sample_100000.csv
# For now getting file from local rather than DBFS


class TaxiTrips(object):
    def __init__(self, spark):
        self.df = (
            spark.read.format("com.databricks.spark.csv")
            .options(header="true", delimiter=";", inferschema="true")
            .load(Config().read_path)
        )

    def show_data(self):
        display(df)

    def get_coordinates_list(self):
        return [
            make_tuple(row.PickUpCoordinates)
            for row in self.df.select("PickUpCoordinates").collect()
        ]
