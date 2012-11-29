import fiona
from .. import gis as gis_util


class FionaShapefileReader(object):
    def __init__(self, shapefile=""):
        self.c = fiona.collection(shapefile, "r")
        self.fields = self.get_fields()
        self.crs = gis_util.proj4_to_wkt(self.c.crs)
        self.shapetype = self.c.schema['geometry'].upper()

    def get_fields(self):
        return self.c.schema['properties'].keys()

    def records(self):
        return self.c.__iter__()

class PyShapefileUtil(object):
    @classmethod
    def get_shapefile_reader(clz, shapefile=""):
        return FionaShapefileReader(shapefile=shapefile)

    @classmethod
    def get_shapefile_writer(clz, shapefile="", driver='ESRI Shapefile', crs=None, 
                             schema=None):
        return fiona.collection(shapefile, "w", driver=driver, crs=crs, 
                                schema=schema)
