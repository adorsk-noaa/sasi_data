import platform
if platform.system() == 'Java':
    from jy_gis import JyGISUtil as gis
else:
    from py_gis import PyGISUtil as gis
