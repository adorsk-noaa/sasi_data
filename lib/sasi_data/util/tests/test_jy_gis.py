import unittest
from test_common_gis import GISCommonTest
from sasi_data.util.jy_gis import JyGISUtil as jy_gis
import json


class JyGISTestCase(unittest.TestCase, GISCommonTest):

    gis = jy_gis

    def generate_polygon_geojson(self):
        return json.dumps({
            "type": "Polygon",
            "coordinates": [
                [[0.0, 0.0], [0.0, 10.0], [1.0, 10.0], [1.0, 0.0], [0.0, 0.0]],
            ]
        })

    def test_get_crs(self):
        epsg = self.gis.get_crs("EPSG:4326")
        wkt = self.gis.get_crs(jy_gis.get_mollweide_crs())
        wkt2 = self.gis.get_crs(wkt)

if __name__ == '__main__':
    unittest.main()
