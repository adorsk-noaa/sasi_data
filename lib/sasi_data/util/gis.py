from pyproj import Proj, transform
from shapely import wkb, wkt
import shapely.geometry as geometry
from shapely.coords import CoordinateSequence


def reproject_shape(shape, p1, p2):
    p1 = get_proj(p1)
    p2 = get_proj(p2)
    if isinstance(shape, geometry.Polygon):
        proj_shape = reproject_polygon(shape, p1, p2)
    elif isinstance(shape, geometry.MultiPolygon):
        proj_shape = reproject_multipolygon(shape, p1, p2)
    elif isinstance(shape, geometry.Point):
        proj_shape = reproject_point(shape, p1, p2)
    return proj_shape

def reproject_polygon(polygon, p1, p2):
    geo_json = { "type": "Polygon", "coordinates": [] }
    new_exterior = reproject_linestring(polygon.exterior, p1, p2)
    geo_json['coordinates'].append(new_exterior)
    for interior_ring in polygon.interiors:
        new_ring = reproject_linestring(interior_ring, p1, p2)
        geo_json['coordinates'].append(new_ring)
    return geometry.shape(geo_json)

def reproject_multipolygon(multipolygon, p1, p2):
    geo_json = { "type": "MultiPolygon", "coordinates": [] }
    for polygon in multipolygon.geoms:
        proj_polygon = reproject_polygon(polygon, p1, p2)
        polygon_coords = [proj_polygon.exterior.coords] + [
            ring.coords for ring in proj_polygon.interiors ]
        geo_json['coordinates'].append(polygon_coords)
    return geometry.shape(geo_json)

def reproject_point(point, p1, p2):
    geo_json = { "type": "Point", "coordinates": [] }
    coords = zip(*point.coords)
    new_coords = transform(p1, p2, *coords)
    geo_json['coordinates'] = zip(*new_coords)
    return geometry.shape(geo_json)

def reproject_linestring(linestring, p1, p2):
    x1, y1 = zip(*linestring.coords)
    x2, y2 = transform(p1, p2, x1, y1)
    return geometry.polygon.LineString(zip(x2, y2))

def get_area(wkb_str="", source_proj="+init=epsg:4326", target_proj=""):
    if not target_proj:
        target_proj = ("+proj=moll +lon_0=0 +x_0=0 +y_0=0 "
                       "+ellps=WGS84 +datum=WGS84 +units=m "
                       "+no_defs")
    shape = wkb.loads(wkb_str)
    proj_shape = reproject_shape(shape, Proj(source_proj), Proj(target_proj))
    return proj_shape.area

def get_intersection(shp1="", shp2=""):
    shape1 = wkb.loads(shp1)
    shape2 = wkb.loads(shp2)
    intersection = shape1.intersection(shape2)
    return wkb.dumps(intersection)

def wkb_to_wkt(wkb_value):
    return wkt.dumps(wkb.loads(wkb_value))

def geojson_to_wkb(geojson):
    return wkb.dumps(geometry.shape(geojson))

def geojson_to_wkt(geojson):
    return wkt.dumps(geometry.shape(geojson))

geojson_to_shape = geometry.shape
shape_to_wkt = wkt.dumps
wkt_to_shape = wkt.loads

def polygon_to_multipolygon(polygon):
    return geometry.MultiPolygon([polygon])

def reproject_wkt(wkt_str, p1, p2):
    shape = wkt.loads(wkt_str)
    proj_shape = reproject_shape(shape, p1, p2)
    return wkt.dumps(proj_shape)

def get_proj(p):
    if isinstance(p, Proj): 
        return p
    elif isinstance(p, dict):
        return Proj(**p)
    elif isinstance(p, str):
        return Proj(p)
