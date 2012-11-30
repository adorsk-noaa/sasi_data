from ..gis import jy_gis
from org.geotools.data import DataStoreFinder
from java.lang import String, Integer, Double
import json


class JyShapefileReader(object):
    def __init__(self, shapefile=""):
        self.ds = DataStoreFinder.getDataStore({
            'url': 'file://%s' % shapefile})
        self.fs = self.ds.getFeatureSource(self.ds.getTypeNames()[0])
        self.schema = self.fs.getSchema()
        self.geom_attr = self.schema.getGeometryDescriptor()

        self.setUpfields()
        self.setUpCrs()
        self.setUpShapeType()

    def setUpfields(self):
        attrs = self.schema.getAttributeDescriptors()
        self.fields = []
        for a in attrs:
            if a is not self.geom_attr:
                name = a.getLocalName()
                self.fields.append(name)

    def setUpCrs(self):
        self.crs = self.schema.getCoordinateReferenceSystem().toWKT()

    def setUpShapeType(self):
        return self.geom_attr.getType().name

    def records(self):
        feature_iterator = self.fs.getFeatures().features()
        while (feature_iterator.hasNext()):
            feature = feature_iterator.next()

            jgeom = feature.getDefaultGeometry()
            geojson_geom = json.loads(jy_gis.GeoJSON.toString(jgeom))

            properties = {}
            for field in self.fields:
                properties[field] = feature.getProperty(field).getValue()

            record = {
                'id': feature.getIdentifier().getID(), 
                'geometry': geojson_geom,
                'properties': properties,
            }
            yield record

class JyShapefileUtil(object):

    @classmethod
    def get_shapefile_reader(clz, shapefile=""):
        return JyShapefileReader(shapefile=shapefile)

    @classmethod
    def get_shapefile_writer(clz, shapefile="", driver='ESRI Shapefile', crs=None, 
                             schema=None):
        return fiona.collection(shapefile, "w", driver=driver, crs=crs, 
                                schema=schema)
