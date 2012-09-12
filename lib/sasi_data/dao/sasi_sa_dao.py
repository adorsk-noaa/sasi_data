import sasi_data.models as sasi_models 
from sa_dao.orm_dao import ORM_DAO
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Text, Float, PickleType, 
                        create_engine, MetaData)
from sqlalchemy.orm import (mapper, relationship)
from geoalchemy import (GeometryExtensionColumn, MultiPolygon, 
                        GeometryColumn, GeometryDDL)


class SASI_SqlAlchemyDAO(object):

    def __init__(self, session=None):
        self.session = session
        self.setUp()

    def setUp(self):
        self.metadata = MetaData()
        self.schema = self.generateSchema()
        self.metadata.create_all(bind=self.session.bind)
        self.orm_dao = ORM_DAO(session=self.session, schema=self.schema)

    def generateSchema(self):
        schema = { 'sources': {} }

        # Cell.
        cell_table = Table('cell', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('type', String),
                           Column('type_id', Integer),
                           Column('area', Float),
                           Column('z', Float),
                           Column('habitat_composition', PickleType),
                           GeometryExtensionColumn('geom', MultiPolygon(2)),
                          )
        GeometryDDL(cell_table)
        mapper(sasi_models.Cell, cell_table, properties = {
            'geom': GeometryColumn(cell_table.c.geom),
        })
        schema['sources']['Cell'] = sasi_models.Cell

        # Habitat.
        habitat_table = Table('habitat', self.metadata,
                      Column('id', Integer, primary_key=True),
                      Column('substrate_id', String),
                      Column('energy_id', String),
                      Column('z', Float),
                      Column('area', Float),
                      GeometryExtensionColumn('geom', MultiPolygon(2)),
                     )
        GeometryDDL(habitat_table)
        mapper(sasi_models.Habitat, habitat_table, properties = {
            'geom': GeometryColumn(habitat_table.c.geom),
        })
        schema['sources']['Habitat'] = sasi_models.Habitat

        # Substrate.
        substrate_table = Table('substrate', self.metadata,
                                Column('id', String, primary_key=True),
                                Column('label', String),
                                Column('description', Text)
                               )
        mapper(sasi_models.Substrate, substrate_table)
        schema['sources']['Substrate'] = sasi_models.Substrate

        # Energy
        energy_table = Table('energy', self.metadata,
                           Column('id', String, primary_key=True),
                           Column('label', String),
                          )
        mapper(sasi_models.Energy, energy_table)
        schema['sources']['Energy'] = sasi_models.Energy

        # Feature.
        feature_table = Table('feature', self.metadata,
                              Column('id', String, primary_key=True),
                              Column('label', String),
                              Column('category', String),
                              Column('description', Text)
                             )
        mapper(sasi_models.Feature, feature_table)
        schema['sources']['Feature'] = sasi_models.Feature

        # Gear.
        gear_table = Table('gear', self.metadata,
                           Column('id', String, primary_key=True),
                           Column('label', String),
                           Column('description', Text)
                          )
        mapper(sasi_models.Gear, gear_table)
        schema['sources']['Gear'] = sasi_models.Gear

        # Vulnerability Assessment.
        va_table = Table('va', self.metadata,
                           Column('gear_id', String, primary_key=True),
                           Column('feature_id', String, primary_key=True),
                           Column('substrate_id', String, primary_key=True),
                           Column('energy_id', String, primary_key=True),
                           Column('s', Integer),
                           Column('r', Integer),
                          )
        mapper(sasi_models.VA, va_table)
        schema['sources']['VA'] = sasi_models.VA

        # Fishing Effort.
        effort_table = Table('effort', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('cell_id', Integer),
                           Column('gear_id', String),
                           Column('swept_area', Float),
                           Column('hours_fished', Float),
                           Column('time', Integer),
                          )
        mapper(sasi_models.Effort, effort_table)
        schema['sources']['Effort'] = sasi_models.Effort

        # Result.
        result_table = Table('result', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('t', Integer),
                           Column('cell_id', Integer),
                           Column('gear_id', String),
                           Column('substrate_id', String),
                           Column('energy_id', String),
                           Column('feature_id', String),
                           Column('a', Float),
                           Column('x', Float),
                           Column('y', Float),
                           Column('z', Float),
                           Column('znet', Float),
                          )
        mapper(sasi_models.Result, result_table)
        schema['sources']['Result'] = sasi_models.Result

        # Model Parameters.
        model_parameters_table = Table('model_parameters', self.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('time_start', Integer),
                                 Column('time_end', Integer),
                                 Column('time_step', Integer),
                                 Column('t_0', Integer),
                                 Column('t_1', Integer),
                                 Column('t_2', Integer),
                                 Column('t_3', Integer),
                                 Column('w_0', Float),
                                 Column('w_1', Float),
                                 Column('w_2', Float),
                                 Column('w_3', Float),
                                 Column('projection', String),
                                )
        mapper(sasi_models.ModelParameters, model_parameters_table)
        schema['sources']['ModelParameters'] = sasi_models.ModelParameters

        return schema

    def tearDown(self):
        # Remove db tables.
        pass

    def save(self, obj, auto_commit=True):
        self.orm_dao.session.add(obj)
        if auto_commit:
            self.orm_dao.session.commit()

    def save_all(self, objs, auto_commit=True):
        self.orm_dao.session.add_all(objs)
        if auto_commit:
            self.orm_dao.session.commit()

    def commit(self):
        self.orm_dao.session.commit()

    def query(self, query_def):
        q = self.orm_dao.get_query(query_def)
        return self.orm_dao.get_result_cursor(q)

