import unittest
from sasi_data.util.sa.tests.db_testcase import DBTestCase
from sasi_data.ingestors.sasi_ingestor import SASI_Ingestor
from sasi_data.util.data_generators import generate_data_dir
from sasi_data.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
import shutil
import logging


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class SASI_Ingestor_TestCase(DBTestCase):
    def setUp(self):
        DBTestCase.setUp(self)

    def tearDown(self):
        if getattr(self, 'data_dir', None):
            shutil.rmtree(self.data_dir)
        DBTestCase.tearDown(self)

    def test_sasi_ingestor(self):
        self.data_dir = generate_data_dir()
        dao = SASI_SqlAlchemyDAO(session=self.session)
        sasi_ingestor = SASI_Ingestor(dao=dao)
        sasi_ingestor.ingest(data_dir=self.data_dir)

    def test_sasi_ingestor_nominal_efforts(self):
        self.data_dir = generate_data_dir(effort_model='nominal')
        dao = SASI_SqlAlchemyDAO(session=self.session)
        sasi_ingestor = SASI_Ingestor(dao=dao)
        sasi_ingestor.ingest(data_dir=self.data_dir)

if __name__ == '__main__':
    unittest.main()
