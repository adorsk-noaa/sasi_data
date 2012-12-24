from sasi_data.ingestors.ingestor import Ingestor
import sasi_data.util.shapefile as shapefile_util
import sasi_data.util.gis as gis_util
import logging


class Shapefile_Ingestor(Ingestor):

    def __init__(self, shapefile=None,
                 geom_attr='geom', shape_attr='shape', force_multipolygon=True,
                 reproject_to=None, **kwargs):
        self.shapefile = shapefile
        self.geom_attr = geom_attr
        self.shape_attr = shape_attr
        self.force_multipolygon = force_multipolygon
        self.reproject_to=reproject_to
        Ingestor.__init__(self, **kwargs)    

    def prepare_records(self):
        self.reader = shapefile_util.get_shapefile_reader(self.shapefile)
        self.records = self.reader.records()

    def pre_ingest(self):
        self.num_records = self.reader.size

    def get_record_attr(self, record, attr):
        return record['properties'].get(attr)

    def map_record(self, record, target, counter):
        super(Shapefile_Ingestor, self).map_record(record, target, counter)
        shape = gis_util.geojson_to_shape(record['geometry'])
        if self.reproject_to:
            shape = gis_util.reproject_shape(shape, self.reader.crs, 
                                             self.reproject_to)
        if shape.geom_type == 'Polygon' and self.force_multipolygon:
            shape = gis_util.polygon_to_multipolygon(shape)
            if self.shape_attr:
                setattr(target, self.shape_attr, shape)
        if self.geom_attr:
            setattr(target, self.geom_attr, gis_util.shape_to_wkt(shape))

    def post_ingest(self, counter):
        self.reader.close()
