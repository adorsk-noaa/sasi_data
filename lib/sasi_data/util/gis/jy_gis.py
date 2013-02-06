from sasi_data.util import memoized
from common_gis import GISUtil
from org.geotools.geojson.geom import GeometryJSON
from org.geotools.referencing import CRS
from org.opengis.referencing.crs import CoordinateReferenceSystem
from org.geotools.geometry.jts import JTS
from com.vividsolutions.jts.io import WKBWriter, WKBReader
from com.vividsolutions.jts.io import WKTWriter, WKTReader
from com.vividsolutions.jts import simplify
import json
from java.lang import System


GeoJSON = GeometryJSON()
wkbw = WKBWriter()
wkbr = WKBReader()
wktw = WKTWriter()
wktr = WKTReader()

# This line is *very* important, it forces coordinates to be in 
# XY order. Some CRS's can have a different order, which leads to
# much head-banging...
# Insert some snarky joke about cartographers and light-bulb changing...
System.setProperty("org.geotools.referencing.forceXY", "true")

class Shape(object):
    """ Wrapper around a JTS Geometry object, to fake
    shapely shape interface. """
    def __init__(self, jgeom):
        self._jgeom = jgeom
        self.geom_type = type(self._jgeom).__name__

    @property
    def area(self):
        return self._jgeom.area

class JyGISUtil(GISUtil):
    @classmethod
    def geojson_to_shape(clz, geojson):
        if isinstance(geojson, dict):
            geojson = json.dumps(geojson)
        return Shape(GeoJSON.read(geojson))

    @classmethod
    def shape_to_geojson(clz, shape):
        return GeoJSON.toString(shape._jgeom)

    @classmethod
    def reproject_shape(clz, shape, crs1, crs2):
        transform = clz.get_transform(crs1, crs2)
        return Shape(JTS.transform(shape._jgeom, transform))

    @classmethod
    def get_intersection(clz, s1, s2):
        if s1._jgeom.intersects(s2._jgeom):
            return Shape(s1._jgeom.intersection(s2._jgeom))
        return None

    @classmethod
    def shape_to_wkt(clz, shape):
        return wktw.write(shape._jgeom)

    @classmethod
    def shape_to_wkb(clz, shape):
        return wkbw.bytesToHex(wkbw.write(shape._jgeom))

    @classmethod
    def wkt_to_shape(clz, wkt_str):
        return Shape(wktr.read(wkt_str))

    @classmethod
    def wkb_to_shape(clz, wkb_str):
        return Shape(wkbr.read(wkbr.hexToBytes(wkb_str)))

    @classmethod
    @memoized
    def get_transform(clz, crs1, crs2):
        crs1 = clz.get_crs(crs1)
        crs2 = clz.get_crs(crs2)
        return CRS.findMathTransform(crs1, crs2, True)

    @classmethod
    @memoized
    def get_crs(clz, crs):
        """ Converts crs definition to GeoTools CRS obj. """
        if isinstance(crs, CoordinateReferenceSystem):
            return crs

        # @TODO
        # Eventually should add handling for
        # proj4 strings, dicts.

        if isinstance(crs, str) or isinstance(crs, unicode):
            if crs.startswith('EPSG:'):
                return CRS.decode(crs)
            # Otherwise assume WKT.
            else:
                return CRS.parseWKT(crs)

    @classmethod
    def get_shape_mbr(clz, shape):
        e = shape._jgeom.getEnvelope()
        coords = e.getCoordinates()
        min_coord = coords[0]
        max_coord = coords[2]
        mbr = (
            min_coord.x,
            min_coord.y,
            max_coord.x,
            max_coord.y,
        )
        return mbr

    @classmethod
    def simplify_shape(clz, shape, tolerance, preserve_topology=True):
        if preserve_topology:
            simplifier = simplify.TopologyPreservingSimplifier
        else:
            simplifier = simplify.DouglasPeuckerSimplifier
        return Shape(shape._jgeom, tolerance)

