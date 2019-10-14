import json


class Config(object):
    def __init__(self):
        self.config_path = "/Users/satrajitmaitra/GeoSpatial-Data-Analysis/config.json"
        with open(self.config_path) as f:
            self.config = json.load(f)
        self.read_path = self._read_path()

    def _read_path(self):
        return self.config.get("PATH")
