import geopandas
import matplotlib.pyplot as plt
import os

from config import Config
from shapely.geometry import Point, Polygon


def display(dataframe):
    ny_map = geopandas.read_file(Config().config.get("SHAPEFILE_PATH"))

    dataframe = dataframe.toPandas()
    geometry = [Point(xy) for xy in zip(dataframe["Longitude"], dataframe["Latitude"])]

    geo_df = geopandas.GeoDataFrame(dataframe, geometry=geometry)
    print(geo_df.head())

    ax = ny_map.plot(figsize=(10, 8), edgecolor="k")
    geo_df[geo_df["prediction"] == 0].plot(ax=ax, color="red")
    geo_df[geo_df["prediction"] == 1].plot(ax=ax, color="blue")
    plt.show()
