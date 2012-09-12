""" Processor functions for processing values during import/export """
import sasi_data.util.gis as gis_util


def sa_wkb_to_wkt(sa_wkb):
    """ Convert SA Geometry proxy objects to wkt. """
    return gis_util.wkb_to_wkt(str(sa_wkb.geom_wkb))
