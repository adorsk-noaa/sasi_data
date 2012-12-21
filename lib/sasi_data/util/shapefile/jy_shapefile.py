from sasi_data.util.gis import jy_gis as jy_gis
from sasi_data.util import gis as gis_util
from org.geotools.data import DataStoreFinder
from org.geotools.feature.simple import (SimpleFeatureTypeBuilder, 
                                         SimpleFeatureBuilder)
from org.geotools.data.shapefile import (ShapefileDataStoreFactory)
from org.geotools.data import DefaultTransaction
from org.geotools.data.collection import ListFeatureCollection
from com.vividsolutions.jts import geom as jts_geom
from org.geotools.geojson.geom import GeometryJSON
from java.lang import String, Integer, Double
from java.util import HashMap
from java.io import File
import json

property_type_mappings = {
    'float': Double,
    'str': String,
    'int': Integer,
}

geom_type_mappings = {}
for geom_type in ['Point', 'LineString', 'Polygon']:
    multi_type = 'Multi' + geom_type
    for type_ in [geom_type, multi_type]:
        geom_type_mappings[type_] = getattr(jts_geom, type_)

ds_factory = ShapefileDataStoreFactory()

class JyShapefileReader(object):
    def __init__(self, shapefile=""):
        self.ds = DataStoreFinder.getDataStore({
            'url': 'file://%s' % shapefile})
        self.fs = self.ds.getFeatureSource(self.ds.getTypeNames()[0])
        self.fc = self.fs.getFeatures()
        self.size = self.fc.size()
        self.feature_iterator = self.fc.features()
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
        while (self.feature_iterator.hasNext()):
            feature = self.feature_iterator.next()

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

    def close(self):
        self.feature_iterator.close()


class JyShapefileUtil(object):

    @classmethod
    def get_shapefile_reader(clz, shapefile=""):
        return JyShapefileReader(shapefile=shapefile)

    @classmethod
    def get_shapefile_writer(clz, shapefile="", driver='ESRI Shapefile', crs=None, 
                             schema=None):

        type_builder = SimpleFeatureTypeBuilder()
        type_builder.setName("custom_feature")
        crs = gis_util.get_crs(crs)
        type_builder.setCRS(crs)
        geom_type = geom_type_mappings[schema['geometry']]
        type_builder.add('geometry', geom_type, crs)
        for prop, property_type in schema.get('properties', {}).items():
            type_builder.add(prop, property_type_mappings[property_type])
        feature_type = type_builder.buildFeatureType()

        feature_builder = SimpleFeatureBuilder(feature_type)

        class ShapefileWriter(object):
            def __init__(self):
                # Set up feature store.
                url = File(shapefile).toURI().toURL()
                ds_params = HashMap()
                ds_params.put("url", url)
                self.ds = ds_factory.createNewDataStore(ds_params)
                self.ds.createSchema(feature_type)
                type_name = self.ds.getTypeNames()[0]
                self.fs = self.ds.getFeatureSource(type_name)
                self.transaction = DefaultTransaction("create")

                # Setup feature list.
                self.features = []

            def write(self, record):
                # Process geometry.
                geom = gis_util.geojson_to_shape(record['geometry'])
                feature_builder.set('geometry', geom)

                # Process properties.
                for prop, value in record.get('properties', {}).items():
                    feature_builder.set(prop, value)

                # Create feature.
                id_ = record.get('id')
                if id_ is not None:
                    id_ = str(id_)
                feature = feature_builder.buildFeature(id_)

                # Add to feature list.
                self.features.append(feature)

            def close(self):
                # Add features to feature store.
                fc = ListFeatureCollection(feature_type, self.features)
                self.fs.setTransaction(self.transaction)
                self.fs.addFeatures(fc)
                self.transaction.commit()
                self.transaction.close()

        return ShapefileWriter()

