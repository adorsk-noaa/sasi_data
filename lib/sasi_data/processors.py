""" Processor functions for processing values during import/export """
import sasi_data.util.gis as gis_util


def wkb_to_wkt(wkb_value):
    return gis_util.wkb_to_wkt(wkb_value)
