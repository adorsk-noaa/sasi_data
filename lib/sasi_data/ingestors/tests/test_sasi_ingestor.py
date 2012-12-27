import unittest
from sasi_data.ingestors.sasi_ingestor import SASI_Ingestor
import sasi_data.util.data_generators as dg
import sasi_data.models as models
from sasi_data.dao.sasi_sa_dao import SASI_SqlAlchemyDAO
import shutil
import logging
import tempfile
import platform


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
#logger.setLevel(logging.INFO)


class SASI_Ingestor_TestCase(unittest.TestCase):
    def setUp(self):
        if platform.system() == 'Java':
            db_uri = 'h2+zxjdbc:///mem:'
        else:
            db_uri = 'sqlite://'
        self.engine = create_engine(db_uri)
        self.connection = self.engine.connect()
        self.session = scoped_session(sessionmaker(bind=self.connection))

    def tearDown(self):
        if getattr(self, 'data_dir', None):
            shutil.rmtree(self.data_dir)

    def test_sasi_ingestor(self):
        self.data_dir = self.generate_data_dir(
            time_start=0,
            time_end=1,
            time_step=1,
        )
        dao = SASI_SqlAlchemyDAO(session=self.session)
        sasi_ingestor = SASI_Ingestor(
            data_dir=self.data_dir, 
            dao=dao,
            hash_cell_size=8,
        )
        sasi_ingestor.ingest()

        substrate_ids = [s.id for s in dao.query('__Substrate').all()]
        self.assertEquals(['S1', 'S2'], sorted(substrate_ids))

        energy_ids = [e.id for e in dao.query('__Energy').all()]
        self.assertEquals(['High', 'Low'], sorted(energy_ids))

        fcat_ids = [fc.id for fc in dao.query('__FeatureCategory').all()]
        self.assertEquals(['FC1', 'FC2'], sorted(fcat_ids))

        feature_ids = [f.id for f in dao.query('__Feature').all()]
        self.assertEquals(['F1', 'F2', 'F3', 'F4'], sorted(feature_ids))

        habs = [h for h in dao.query('__Habitat').all()]

        cells = [c for c in dao.query('__Cell').all()]
        expected_composition = {
            ('S1', 'High'): .25,
            ('S1', 'Low'): .25,
            ('S2', 'High'): .25,
            ('S2', 'Low'): .25,
        }
        for c in cells:
            for key, v in c.habitat_composition.items():
                self.assertAlmostEquals(
                    expected_composition[key],
                    v
                )

    def test_sasi_ingestor_nominal_efforts(self):
        t0 = 0
        tf = 1
        dt = 1
        self.data_dir = self.generate_data_dir(
            time_start=t0, 
            time_end=tf,
            time_step=dt,
            effort_model='nominal'
        )
        dao = SASI_SqlAlchemyDAO(session=self.session)
        sasi_ingestor = SASI_Ingestor(data_dir=self.data_dir, dao=dao)
        sasi_ingestor.ingest()

        cells = dao.query('__Cell').all()
        gears = dao.query('__Gear').all()
        num_gears = len(gears)
        actual_efforts = dao.query('__Effort').all()
        expected_efforts = []
        for t in range(t0, tf, dt):
            for c in cells:
                for g in gears:
                    expected_efforts.append(models.Effort(
                        cell_id=c.id,
                        time=t,
                        a=c.area/num_gears,
                        gear_id=g.id
                    ))

        def get_keyed_efforts(efforts):
            keyed_efforts = {}
            for e in efforts:
                effort_key = (e.cell_id, e.time, e.gear_id,)
                efforts_for_key = keyed_efforts.setdefault(effort_key, [])
                efforts_for_key.append(e)
            return keyed_efforts

        actual_keyed_efforts = get_keyed_efforts(actual_efforts)
        expected_keyed_efforts = get_keyed_efforts(expected_efforts)
        for effort_key, e_efforts in expected_keyed_efforts.items():
            a_efforts = actual_keyed_efforts[effort_key]
            actual_total_a = sum([e.a for e in a_efforts])
            expected_total_a = sum([e.a for e in e_efforts])
            self.assertTrue(actual_total_a)
            self.assertEquals(actual_total_a, expected_total_a)

    def generate_data_dir(self, **kwargs):
        data = {}

        data['substrates'] = [
            models.Substrate(id="S1"),
            models.Substrate(id="S2"),
        ]

        data['energys'] = [
            models.Energy(id="High"), 
            models.Energy(id="Low")
        ]

        data['feature_categories'] = [
            models.FeatureCategory(id="FC1"), 
            models.FeatureCategory(id="FC2"), 
        ]

        data['features'] = [
            models.Feature(id="F1", category="FC1"), 
            models.Feature(id="F2", category="FC2"), 
            models.Feature(id="F3", category="FC1"), 
            models.Feature(id="F4", category="FC2"), 
        ]

        data['grid'] = [
            dg.generate_cell(
                id=0,
                x0=0,y0=-2,x1=4, y1=0,
            ),
            dg.generate_cell(
                id=1,
                x0=0,y0=0,x1=4, y1=2,
            )
        ]

        data['habitats'] = [
            dg.generate_habitat(
                id=1,
                x0=0,y0=-2,x1=1, y1=2,
                substrate_id='S1',
                energy_id='Low',
                z=10
            ),
            dg.generate_habitat(
                id=2,
                x0=1,y0=-2,x1=2, y1=2,
                substrate_id='S1',
                energy_id='High',
                z=20
            ),
            dg.generate_habitat(
                id=3,
                x0=2,y0=-2,x1=3, y1=2,
                substrate_id='S2',
                energy_id='Low',
                z=30
            ),
            dg.generate_habitat(
                id=4,
                x0=3,y0=-2,x1=4, y1=2,
                substrate_id='S2',
                energy_id='High',
                z=40
            ),
        ]

        return dg.generate_data_dir(data=data, **kwargs)

if __name__ == '__main__':
    unittest.main()
