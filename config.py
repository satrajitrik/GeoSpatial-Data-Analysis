import json
import os


class Config(object):
    def __init__(self):
        config_path = "/Users/satrajitmaitra/GeoSpatial-Data-Analysis/config.json"
        with open(config_path) as f:
            self.config = json.load(f)

    def get_df_from_parquet(self, spark):
        df_from_csv = (
            spark.read.format("com.databricks.spark.csv")
            .options(header="true", delimiter=";", inferschema="true")
            .load(self.config.get("CSV_PATH"))
        )

        if not os.listdir(self.config.get("DATA_FOLDER_PATH")):
            self._convert_csv_df_to_parquet(df_from_csv)
        return spark.read.load(self.config.get("PARQUET_FILE_PATH"))

    """
        Converting csv to parquet because of lazy execution.
        Lazy execution not supported while reading csv files.
    """

    def _convert_csv_df_to_parquet(self, df_from_csv):
        df_from_csv.write.save(self.config.get("PARQUET_FILE_PATH"), format="parquet")
