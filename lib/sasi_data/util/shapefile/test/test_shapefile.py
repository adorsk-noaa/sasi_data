import unittest
from sasi_data.util import shapefile as shapefile_util
from sasi_data.util import data_generators as dg
import os
import tempfile


this_dir = os.path.dirname(os.path.abspath(__file__))

class ShapefileTestCase(unittest.TestCase):

    def test_read_shapefile(self):
        test_shapefile = os.path.join(this_dir, "test_layer", "layer.shp")
        reader = shapefile_util.get_shapefile_reader(test_shapefile)
        for r in reader.records():
            print r

    def test_write_multipolygon_shapefile(self):
        hndl, shapefile = tempfile.mkstemp(suffix=".shp")
        schema = {
            'geometry': 'MultiPolygon',
            'properties': {
                'INT': 'int',
                'FLOAT': 'float',
                'STRING': 'str',
            }
        }
        records = []
        for i in range(1, 4):
            coords = [[dg.generate_polygon_coords(x=i, y=i)]]
            records.append({
                'id': i,
                'geometry': {
                    'type': 'MultiPolygon',
                    'coordinates': coords
                },
                'properties': {
                    'INT': i,
                    'FLOAT': float(i),
                    'STRING': str(i),
                }
            })
        writer = shapefile_util.get_shapefile_writer(
            shapefile=shapefile,
            crs='EPSG:4326',
            schema=schema,
        )
        for record in records:
            writer.write(record)
        writer.close()
        print "shp: ", shapefile

if __name__ == '__main__':
    unittest.main()
