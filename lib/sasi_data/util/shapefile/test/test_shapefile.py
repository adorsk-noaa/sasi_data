import unittest
from sasi_data.util import shapefile as shapefile_util
import os


this_dir = os.path.dirname(os.path.abspath(__file__))

class ShapefileTestCase(unittest.TestCase):

    def test_read_shapefile(self):
        test_shapefile = os.path.join(this_dir, "test_layer", "layer.shp")
        reader = shapefile_util.get_shapefile_reader(test_shapefile)
        for r in reader.records():
            print r

if __name__ == '__main__':
    unittest.main()
