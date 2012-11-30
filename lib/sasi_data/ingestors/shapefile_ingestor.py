import sasi_data.util.shapefile as shapefile_util
import sasi_data.util.gis as gis_util
import logging


class Shapefile_Ingestor(object):

    def __init__(self, shp_file=None, dao=None, clazz=None, mappings={},
                 geom_attr='geom', force_multipolygon=True,
                 reproject_to=None, logger=logging.getLogger(),
                 limit=None):
        self.dao = dao
        self.clazz = clazz
        self.mappings = mappings
        self.reader = shapefile_util.get_shapefile_reader(shp_file)
        self.geom_attr = geom_attr
        self.force_multipolygon = force_multipolygon
        self.reproject_to=reproject_to
        self.logger = logger
        self.limit = limit

    def ingest(self, log_interval=1000, auto_commit=True):
        fields = self.reader.fields
        records = [r for r in self.reader.records()]
        num_records = len(records)
        counter = 0
        limit = self.limit or num_records
        for record in records[:limit]:
            counter += 1
            if ((counter % log_interval) == 0):
                self.logger.info(" %d of %d (%.1f%%)" % (
                    counter, limit, (1.0 * counter/limit) * 100))
            obj = self.clazz()

            for mapping in self.mappings:
                raw_value = record['properties'].get(mapping.get('source'))
                if raw_value is None:
                    raw_value = record['properties'].get(mapping.get('source').lower())

                processor = mapping.get('processor')
                if not processor:
                    processor = lambda value: value
                value = processor(raw_value)
                setattr(obj, mapping['target'], value)

            shape = gis_util.geojson_to_shape(record['geometry'])

            if self.reproject_to:
                shape = gis_util.reproject_shape(shape, self.reader.crs, 
                                                 self.reproject_to)
            if shape.geom_type == 'Polygon' and self.force_multipolygon:
                shape = gis_util.polygon_to_multipolygon(shape)

            if self.geom_attr and hasattr(obj, self.geom_attr):
                setattr(obj, self.geom_attr, gis_util.shape_to_wkt(shape))

            self.dao.save(obj, auto_commit=auto_commit)
