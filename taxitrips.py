from ast import literal_eval as make_tuple
from config import Config


class TaxiTrips(object):
    """
        Reading from parquet because of lazy execution
    """

    def __init__(self, spark):
        self.df = Config().get_df_from_parquet(spark)

    def show_data(self):
        display(self.df)

    def get_coordinates_list(self):
        return [
            make_tuple(row.PickUpCoordinates)
            for row in self.df.select("PickUpCoordinates").collect()
        ]
