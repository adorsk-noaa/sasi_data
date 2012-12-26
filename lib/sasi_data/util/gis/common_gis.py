import json


class GISUtil(object):

    @classmethod
    def reproject_shape(clz, shape, crs1, crs2):
        pass

    @classmethod
    def get_mollweide_crs(clz):
        return """
        PROJCS["WORLD_MOLLWEIDE", 
          GEOGCS["WGS_1984", 
            DATUM["WGS_1984", 
              SPHEROID["WGS_1984", 6378137.0, 298.257223563]], 
            PRIMEM["GREENWICH", 0.0], 
            UNIT["degree", 0.017453292519943295], 
            AXIS["Longitude", EAST], 
            AXIS["Latitude", NORTH]], 
          PROJECTION["Mollweide"], 
          PARAMETER["semi_minor", 6378137.0], 
          PARAMETER["central_meridian", 0.0], 
          UNIT["m", 1.0], 
          AXIS["x", EAST], 
          AXIS["y", NORTH]]
        """

    get_default_geographic_crs = get_mollweide_crs

    @classmethod
    def get_shape_area(clz, shape, source_crs="EPSG:4326", target_crs=""):
        if not target_crs:
            target_crs = clz.get_default_geographic_crs()
        proj_shape = clz.reproject_shape(shape, source_crs, target_crs)
        return proj_shape.area

    @classmethod
    def geojson_to_wkb(clz, geojson):
        return clz.shape_to_wkb(clz.geojson_to_shape(geojson))

    @classmethod
    def wkb_to_geojson(clz, wkb):
        return clz.shape_to_geojson(clz.wkb_to_shape(wkb))

    @classmethod
    def geojson_to_wkt(clz, geojson):
        return clz.shape_to_wkt(geojson_to_shape(geojson))

    @classmethod
    def polygon_to_multipolygon(clz, polygon):
        geojson = json.loads(clz.shape_to_geojson(polygon))
        geojson['type'] = 'MultiPolygon'
        geojson['coordinates'] = [geojson['coordinates']]
        return clz.geojson_to_shape(json.dumps(geojson))

    @classmethod
    def wkb_to_wkt(clz, wkb):
        shape = clz.wkb_to_shape(wkb)
        return clz.shape_to_wkt(shape)
