import unittest
from sasi_data.util.gis.py_gis import PyGISUtil as py_gis
from test_common_gis import GISCommonTest


class PyGISTestCase(unittest.TestCase, GISCommonTest):
    gis = py_gis

if __name__ == '__main__':
    unittest.main()
