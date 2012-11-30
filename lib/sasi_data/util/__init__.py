import platform
import collections
import functools


class memoized(object):
    """ Memoization decorator. """
    def __init__(self, func):
        self.func = func
        self.cache = {}
    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        else:
            value = self.func(*args)
            self.cache[args] = value
            return value
    def __repr__(self):
        return self.func.__doc__
    def __get__(self, obj, objtype):
        return functools.partial(self.__call__, obj)


if platform.system() == 'Java':
    from .gis.jy_gis import JyGISUtil as gis
    from .shapefile.jy_shapefile import JyShapefileUtil as shapefile
else:
    from .gis.py_gis import PyGISUtil as gis
    from .shapefile.py_shapefile import PyShapefileUtil as shapefile
