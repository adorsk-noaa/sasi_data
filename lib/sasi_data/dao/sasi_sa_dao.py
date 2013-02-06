import sasi_data.models as sasi_models 
from sa_dao.orm_dao import ORM_DAO
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Text, Float, PickleType, 
                        create_engine, MetaData)
from sqlalchemy.orm import (mapper, relationship)
import sys
import logging


class SASI_SqlAlchemyDAO(ORM_DAO):

    def __init__(self, session=None, create_tables=True, **kwargs):
        self.session = session
        self.setUp()
        ORM_DAO.__init__(self, session=self.session, schema=self.schema,
                         **kwargs)
        if create_tables:
            self.create_tables()

    def setUp(self):
        self.metadata = MetaData()
        self.schema = self.generateSchema()

    def get_local_mapped_class(self, base_class, table, local_name, **kw):
        local_class = type(local_name, (base_class,), {})
        mapper(local_class, table, **kw)
        return local_class

    def create_tables(self, bind=None):
        if not bind:
            bind = self.session.bind
        self.metadata.create_all(bind=bind)

    def generateSchema(self):
        schema = { 'sources': {} }

        # Define tables and mappings.
        mappings = {}

        # Cell.
        mappings['Cell'] = {
            'table': Table('cell', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('area', Float),
                           Column('z', Float),
                           Column('habitat_composition', PickleType),
                           Column('geom_wkt', String),
                          ),
        }

        # Habitat.
        mappings['Habitat'] = {
            'table': Table('habitat', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('substrate_id', String),
                           Column('energy_id', String),
                           Column('z', Float),
                           Column('area', Float),
                          ),
        }

        # Substrate.
        mappings['Substrate'] = {
            'table': Table('substrate', self.metadata,
                           Column('id', String, primary_key=True),
                           Column('label', String),
                           Column('description', Text)
                          ),
        }

        # Energy
        mappings['Energy'] = {
            'table' : Table('energy', self.metadata,
                            Column('id', String, primary_key=True),
                            Column('label', String),
                           ),
        }

        # Feature Category.
        mappings['FeatureCategory'] = {
            'table' : Table('feature_category', self.metadata,
                            Column('id', String, primary_key=True),
                            Column('label', String),
                            Column('description', Text)
                           ),
        }

        # Feature.
        mappings['Feature'] = {
            'table' : Table('feature', self.metadata,
                            Column('id', String, primary_key=True),
                            Column('label', String),
                            Column('category', String),
                            Column('description', Text)
                           ),
        }

        # Gear.
        mappings['Gear'] = {
            'table': Table('gear', self.metadata,
                           Column('id', String, primary_key=True),
                           Column('generic_id', String),
                           Column('is_generic', String),
                           Column('label', String),
                           Column('description', Text),
                           Column('min_depth', Float),
                           Column('max_depth', Float),
                          )
        }

        # Vulnerability Assessment.
        mappings['VA'] = {
            'table': Table('va', self.metadata,
                           Column('gear_id', String, primary_key=True),
                           Column('feature_id', String, primary_key=True),
                           Column('substrate_id', String, primary_key=True),
                           Column('energy_id', String, primary_key=True),
                           Column('s', Integer),
                           Column('r', Integer),
                          ),
        }

        # Fishing Effort.
        mappings['Effort'] = {
            'table': Table('effort', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('time', Integer),
                           Column('cell_id', Integer),
                           Column('gear_id', String),
                           Column('a', Float),
                           Column('hours_fished', Float),
                           Column('value', Float),
                          ),
        }

        # Model Parameters.
        mappings['ModelParameters'] = {
            'table' : Table('model_parameters', self.metadata,
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
                            Column('effort_model', String),
                            Column('projection', String),
                           ),
        }

        # Result.
        mappings['SasiResult'] = {
            'table': Table('sasi_result', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('t', Integer),
                           Column('cell_id', Integer),
                           Column('gear_id', String),
                           Column('substrate_id', String),
                           Column('energy_id', String),
                           Column('feature_id', String),
                           Column('feature_category_id', String),
                           Column('a', Float),
                           Column('x', Float),
                           Column('y', Float),
                           Column('z', Float),
                           Column('znet', Float),
                          ),
        }

        # Fishing Results.
        mappings['FishingResult'] = {
            'table': Table('fishing_result', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('t', Integer),
                           Column('cell_id', Integer),
                           Column('gear_id', String),
                           Column('a', Float),
                           Column('value', Float),
                           Column('value_net', Float),
                           Column('hours_fished', Float),
                           Column('hours_fished_net', Float),
                          ),
        }


        for class_name, mapping in mappings.items():
            mapped_class = self.get_local_mapped_class(
                getattr(sasi_models, class_name),
                mapping['table'],
                class_name,
                **mapping.get('mapper_kwargs', {})
            )
            schema['sources'][class_name] = mapped_class

        return schema

    def bulk_insert_objects(self, source, objects, batch_size=1e4, commit=True):
        table = self.get_table_for_class(
            self.schema['sources'][source])
        cols = [c for c in table.c.keys()]

        def obj_dicts():
            for o in objects:
                yield dict(zip(cols, [getattr(o, c, None) for c in cols]))

        self.save_dicts(source, obj_dicts(), batch_size=batch_size, 
                        commit=commit)
