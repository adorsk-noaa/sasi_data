import json
import unittest
from sasi_data.util import gis as gis_util


class GISCommonTest(object):

    def generate_rect_geojson(self, c=[0,0,1,1]):
        return json.dumps({
            "type": "Polygon",
            "coordinates": [[
                (c[0], c[1]), (c[0], c[3]), (c[2], c[3]), (c[2], c[1]), 
                (c[0], c[1]),
            ]]
        })

class GISTestCase(unittest.TestCase, GISCommonTest):

    def test_geojson_to_shape(self):
        poly_geojson = self.generate_rect_geojson()
        shape = gis_util.geojson_to_shape(poly_geojson)
        self.assertEquals(shape.geom_type , 'Polygon')

    def test_reproject_shape(self):
        geojson = self.generate_rect_geojson()
        shape = gis_util.geojson_to_shape(geojson)
        reprojected_shape = gis_util.reproject_shape(shape, 
                                                   'EPSG:4326', 
                                                     gis_util.get_mollweide_crs()
                                                    )
    def test_get_shape_area(self):
        geojson = self.generate_rect_geojson()
        shape = gis_util.geojson_to_shape(geojson)
        area = gis_util.get_shape_area(shape)

    def test_get_intersection(self):
        pol1 = self.generate_rect_geojson([0,0,2,1])
        pol2 = self.generate_rect_geojson([1,0,3,1])
        shp1 = gis_util.geojson_to_shape(pol1)
        shp2 = gis_util.geojson_to_shape(pol2)
        intersection = gis_util.get_intersection(shp1, shp2)
        gis_util.shape_to_wkt(intersection)

    def test_polygon_to_multipolygon(self):
        poly_geojson = self.generate_rect_geojson()
        poly_shape = gis_util.geojson_to_shape(poly_geojson)
        multipoly_shape = gis_util.polygon_to_multipolygon(poly_shape)
        self.assertEquals(multipoly_shape.geom_type, 'MultiPolygon')

if __name__ == '__main__':
    unittest.main()
