from common_gis import GISUtil
from pyproj import Proj, transform
from shapely import wkb, wkt
import shapely.geometry as geometry
from shapely.coords import CoordinateSequence
from osgeo import osr
import json
import re


class PyGISUtil(GISUtil):
    @classmethod
    def geojson_to_shape(clz, geojson):
        if isinstance(geojson, str):
            geojson = json.loads(geojson)
        return geometry.shape(geojson)

    @classmethod
    def shape_to_geojson(clz, shape):
        return json.dumps(geometry.mapping(shape))

    @classmethod
    def reproject_shape(clz, shape, crs1, crs2):
        crs1 = clz.get_crs(crs1)
        crs2 = clz.get_crs(crs2)
        if isinstance(shape, geometry.Polygon):
            proj_shape = clz.reproject_polygon(shape, crs1, crs2)
        elif isinstance(shape, geometry.MultiPolygon):
            proj_shape = clz.reproject_multipolygon(shape, crs1, crs2)
        elif isinstance(shape, geometry.Point):
            proj_shape = clz.reproject_point(shape, crs1, crs2)
        return proj_shape

    @classmethod
    def reproject_polygon(clz, polygon, crs1, crs2):
        geo_json = { "type": "Polygon", "coordinates": [] }
        new_exterior = clz.reproject_linestring(polygon.exterior, crs1, crs2)
        geo_json['coordinates'].append(new_exterior)
        for interior_ring in polygon.interiors:
            new_ring = clz.reproject_linestring(interior_ring, crs1, crs2)
            geo_json['coordinates'].append(new_ring)
        return geometry.shape(geo_json)

    @classmethod
    def reproject_multipolygon(clz, multipolygon, crs1, crs2):
        geo_json = { "type": "MultiPolygon", "coordinates": [] }
        for polygon in multipolygon.geoms:
            proj_polygon = clz.reproject_polygon(polygon, crs1, crs2)
            polygon_coords = [proj_polygon.exterior.coords] + [
                ring.coords for ring in proj_polygon.interiors ]
            geo_json['coordinates'].append(polygon_coords)
        return geometry.shape(geo_json)

    @classmethod
    def reproject_point(clz, point, crs1, crs2):
        geo_json = { "type": "Point", "coordinates": [] }
        coords = zip(*point.coords)
        new_coords = transform(crs1, crs2, *coords)
        geo_json['coordinates'] = zip(*new_coords)
        return geometry.shape(geo_json)

    @classmethod
    def reproject_linestring(clz, linestring, crs1, crs2):
        x1, y1 = zip(*linestring.coords)
        x2, y2 = transform(crs1, crs2, x1, y1)
        return geometry.polygon.LineString(zip(x2, y2))

    @classmethod
    def get_intersection(clz, shape1, shape2):
        if shape1.intersects(shape2):
            return shape1.intersection(shape2)
        return None

    @classmethod
    def shape_to_wkt(clz, shape):
        return wkt.dumps(shape)

    @classmethod
    def shape_to_wkb(clz, shape):
        return wkb.dumps(shape)

    @classmethod
    def wkt_to_shape(clz, shape):
        return wkt.loads(shape)

    @classmethod
    def wkb_to_shape(clz, shape):
        return wkb.loads(shape)

    @classmethod
    def wkb_to_wkt(clz, wkb_value):
        return wkt.dumps(wkb.loads(wkb_value))

    @classmethod
    def get_crs(clz, crs):
        """ Converts crs definition to pyproj obj. """
        if isinstance(crs, Proj): 
            return crs
        elif isinstance(crs, dict):
            return Proj(**crs)
        elif isinstance(crs, str):
            # Convert WKT crs string to proj4 string.
            if re.match('\s*(PROJ|GEOG)CS', crs):
                srs = osr.SpatialReference()
                srs.ImportFromWkt(crs)
                crs = srs.ExportToProj4()
            # Convert EPSG code to proj4 string.
            elif crs.startswith('EPSG:'):
                epsg, epsg_code = crs.split(':')
                crs = '+init=epsg:%s' % epsg_code
            return Proj(crs)

    @classmethod
    def proj4_to_wkt(clz, proj4_crs):
        if isinstance(proj4_crs, dict):
            proj4_crs = ' '.join(["+%s=%s" % item for item in proj4_crs.items()])
        srs = osr.SpatialReference()
        srs.ImportFromProj4(proj4_crs)
        return srs.ExportToWkt()

    @classmethod
    def wkt_to_proj4(clz, wkt_crs):
        srs = osr.SpatialReference()
        srs.ImportFromWkt(proj4_crs)
        return srs.ExportToProj4()
