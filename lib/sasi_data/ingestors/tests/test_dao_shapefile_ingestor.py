import unittest
from sasi_data.util.sa.tests.db_testcase import DBTestCase
from sasi_data.ingestors.ingestor import Ingestor
from sasi_data.ingestors.dao_writer import DAOWriter 
from sasi_data.ingestors.shapefile_reader import ShapefileReader
from sasi_data.ingestors.mapper import ClassMapper
from sa_dao.orm_dao import ORM_DAO
from sasi_data.util import shapefile as shapefile_util
import sasi_data.util.gis as gis_util
from sasi_data.util import data_generators as dg
from sqlalchemy import Table, Column, Integer, String
from geoalchemy import GeometryColumn, MultiPolygon, GeometryDDL
from sqlalchemy.ext.declarative import declarative_base
import tempfile
import os
import time
import logging


class DAO_Shapefile_Ingestor_TestCase(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)

    def test_shapefile_ingestor(self):
        Base = declarative_base()

        class TestClass(Base):
            __tablename__ = 'testclass'
            id = Column(Integer, primary_key=True)
            attr1 = Column(Integer) 
            attr2 = Column(String)
            geom = GeometryColumn(MultiPolygon(2))
        GeometryDDL(TestClass.__table__)
        schema = {
            'sources': {
                'TestClass': TestClass
            }
        }

        Base.metadata.create_all(self.connection)

        dao = ORM_DAO(session=self.session, schema=schema)

        shapedir = tempfile.mkdtemp()
        shapefile = os.path.join(shapedir, "test.shp")
        schema = {
            'geometry': 'MultiPolygon',
            'properties': {
                'S_ATTR1': 'int',
                'S_ATTR2': 'str',
            }
        }
        records = []
        for i in range(5):
            coords = [[dg.generate_polygon_coords(x=i, y=i)]]
            records.append({
                'id': i,
                'geometry': {
                    'type': 'MultiPolygon',
                    'coordinates': coords
                },
                'properties': {
                    'S_ATTR1': i,
                    'S_ATTR2': str(i),
                }
            })
        writer = shapefile_util.get_shapefile_writer(
            shapefile=shapefile,
            crs='EPSG:4326',
            schema=schema,
        )
        for record in records:
            writer.write(record)
        writer.close()

        mappings = [
            {
                'source': 'S_ATTR1', 
                'target': 'attr1',
                'processor': lambda value: int(value) * 10
            },
            {
                'source': 'S_ATTR2', 
                'target': 'attr2',
            },
            {
                'source': '__shape',
                'target': 'geom',
                'processor': gis_util.shape_to_wkt,
            }
        ]

        Ingestor(
            reader=ShapefileReader(shp_file=shapefile),
            processors=[
                ClassMapper(clazz=TestClass, mappings=mappings),
                DAOWriter(dao=dao, commit=False),
            ]
        ).ingest()
        results = dao.query({
            'SELECT': ['__TestClass']
        }).all()

        for r in results:
            print r.attr1, r.attr2, dao.session.scalar(r.geom.wkt)


if __name__ == '__main__':
    unittest.main()
