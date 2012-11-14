import sasi_data.util.shapefile as shapefile_util
import sasi_data.util.gis as gis_util


class Shapefile_Ingestor(object):

    def __init__(self, shp_file=None, dao=None, clazz=None, mappings={},
                 geom_attr='geom', force_multipolygon=True,
                 reproject_to=None):
        self.dao = dao
        self.clazz = clazz
        self.mappings = mappings
        self.reader = shapefile_util.get_shapefile_reader(shp_file)
        self.geom_attr = geom_attr
        self.force_multipolygon = force_multipolygon
        self.reproject_to=reproject_to

    def ingest(self):
        fields = self.reader.fields
        for record in self.reader.records():
            obj = self.clazz()

            for mapping in self.mappings:
                raw_value = (record['properties'].get(mapping.get('source')) or
                             record['properties'].get(mapping.get('source').lower())
                            )

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

            self.dao.save(obj)
