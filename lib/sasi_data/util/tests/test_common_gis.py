import json


class GISCommonTest(object):

    gis = None

    def generate_rect_geojson(self, c=[0,0,1,1]):
        return json.dumps({
            "type": "Polygon",
            "coordinates": [[
                (c[0], c[1]), (c[0], c[3]), (c[2], c[3]), (c[2], c[1]), 
                (c[0], c[1]),
            ]]
        })

    def test_geojson_to_shape(self):
        poly_geojson = self.generate_rect_geojson()
        shape = self.gis.geojson_to_shape(poly_geojson)
        self.assertEquals(shape.geom_type , 'Polygon')

    def test_reproject_shape(self):
        geojson = self.generate_rect_geojson()
        shape = self.gis.geojson_to_shape(geojson)
        reprojected_shape = self.gis.reproject_shape(shape, 
                                                   'EPSG:4326', 
                                                     self.gis.get_mollweide_crs()
                                                    )
    def test_get_shape_area(self):
        geojson = self.generate_rect_geojson()
        shape = self.gis.geojson_to_shape(geojson)
        area = self.gis.get_shape_area(shape)

    def test_get_intersectino(self):
        pol1 = self.generate_rect_geojson([0,0,2,1])
        pol2 = self.generate_rect_geojson([1,0,3,1])
        shp1 = self.gis.geojson_to_shape(pol1)
        shp2 = self.gis.geojson_to_shape(pol2)
        intersection = self.gis.get_intersection(shp1, shp2)
        self.gis.shape_to_wkt(intersection)
