import unittest
from sasi_data.exporters import CSV_Exporter
from StringIO import StringIO


class CSV_Ingestor_TestCase(unittest.TestCase):

    def test_csv_exporter(self):

        class TestClass(object):
            def __init__(self, attr1, attr2):
                self.attr1 = attr1
                self.attr2 = attr2

        objects = []
        for i in range(3):
            objects.append(TestClass(i, "attr2_%s" % i))

        mappings = [
            {
                'source': 'attr1', 
                'target': 'attr1_t',
                'processor': lambda value: int(value) * 10
            },
            'attr2',
        ]

        csv_file = StringIO()

        csv_exporter = CSV_Exporter(
            csv_file=csv_file, 
            objects=objects,
            mappings=mappings
        )
        csv_exporter.export()

if __name__ == '__main__':
    unittest.main()
