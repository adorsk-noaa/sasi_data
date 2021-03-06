import unittest
from sasi_data.util.sa.tests.db_testcase import DBTestCase
from sasi_data.ingestors.ingestor import Ingestor
from sasi_data.ingestors.dao_writer import DAOWriter 
from sasi_data.ingestors.csv_reader import CSVReader
from sasi_data.ingestors.mapper import ClassMapper
from sa_dao.orm_dao import ORM_DAO
from StringIO import StringIO
import csv
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base


class CSV_Ingestor_TestCase(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)

    def test_dao_csv_ingestor(self):
        Base = declarative_base()

        class TestClass(Base):
            __tablename__ = 'testclass'
            id = Column(Integer, primary_key=True)
            attr1 = Column(Integer) 
            attr2 = Column(String)
        Base.metadata.create_all(self.connection)
        schema = {
            'sources': {
                'TestClass': TestClass
            }
        }

        dao = ORM_DAO(session=self.session, schema=schema)

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

        Ingestor(
            reader=CSVReader(csv_file=csv_file),
            processors=[
                ClassMapper(clazz=TestClass, mappings=mappings),
                DAOWriter(dao=dao, commit=False),
            ]
        ).ingest()
        results = dao.query({
            'SELECT': ['__TestClass']
        }).all()
        for r in results: print r.__dict__

if __name__ == '__main__':
    unittest.main()
