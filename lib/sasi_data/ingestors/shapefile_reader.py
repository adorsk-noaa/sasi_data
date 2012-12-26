import csv
import sasi_data.util.shapefile as shapefile_util
import sasi_data.util.gis as gis_util


class ShapefileReader(object):
    """ Reads GeoJSON records from shapefiles.
    Special '__shape' attr can be used to access
    shape. Otherwise attrs access GeoJSON dict's properties.
    """
    def __init__(self, shp_file=None, force_multipolygon=True,
                 reproject_to=None, **kwargs):
        self.shp_file = shp_file
        self.force_multipolygon = force_multipolygon
        self.reproject_to = reproject_to
        self.reader = shapefile_util.get_shapefile_reader(self.shp_file)

    def get_records(self):
        for geojson in self.reader.records():
            record = {}
            record.update(geojson.get('properties', {}))
            shape = gis_util.geojson_to_shape(geojson['geometry']))
            if self.reproject_to:
                shape = gis_util.reproject_shape(
                    shape, self.reader.get_crs(), self.reproject_to)
            if shape.geom_type == 'Polygon' and self.force_multipolygon:
                shape = gis_util.polygon_to_multipolygon(shape)
            record['__shape'] = shape
            yield record

    @property
    def size(self):
        if not hasattr(self, '_size'):
            self._size = self.reader.size
        return self._size

    def close(self):
        self.reader.close()
