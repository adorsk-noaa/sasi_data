import unittest
from sasi_data.ingestors.dict_csv_ingestor import Dict_CSV_Ingestor
from StringIO import StringIO
import csv


class Dict_CSV_Ingestor_TestCase(unittest.TestCase):

    def test_dict_csv_ingestor(self):
        csv_data = StringIO()
        writer = csv.DictWriter(csv_data, fieldnames=['s_attr1', 's_attr2'])
        writer.writeheader()
        for i in range(5):
            record = {
                's_attr1': i,
                's_attr2': "s_attr2_%s" % i,
            }
            writer.writerow(record)
        csv_file = StringIO(csv_data.getvalue())

        mappings = [
            {
                'source': 's_attr1', 
                'target': 'attr1',
                'processor': lambda value: int(value) * 10
            },

            {
                'source': 's_attr2', 
                'target': 'attr2',
            },
        ]

        ingestor = Dict_CSV_Ingestor(
            csv_file=csv_file, 
            mappings=mappings
        )
        ingestor.ingest()
        print ingestor.records

if __name__ == '__main__':
    unittest.main()
