import platform


if platform.system() == 'Java':
    from .gis.jy_gis import JyGISUtil as gis
    from .shapefile.jy_shapefile import JyShapefileUtil as shapefile
else:
    from .gis.py_gis import PyGISUtil as gis
    from .shapefile.py_shapefile import PyShapefileUtil as shapefile
