import sasi_data.models as sasi_models 
from sa_dao.orm_dao import ORM_DAO
from sqlalchemy import (Table, Column, ForeignKey, ForeignKeyConstraint, 
                        Integer, String, Text, Float, PickleType, 
                        create_engine, MetaData)
from sqlalchemy.orm import (mapper, relationship)
from geoalchemy import (GeometryExtensionColumn, MultiPolygon, 
                        GeometryColumn, GeometryDDL)
from geoalchemy import functions as geo_funcs
import sys
import logging


class SASI_SqlAlchemyDAO(object):

    def __init__(self, session=None, create_tables=True):
        self.session = session
        self.setUp()
        if create_tables:
            self.create_tables()

    def setUp(self):
        self.metadata = MetaData()
        self.schema = self.generateSchema()
        self.orm_dao = ORM_DAO(session=self.session, schema=self.schema)
        self.orm_dao.valid_funcs.append('func.st_intersects')
        self.orm_dao.valid_funcs.append('geo_funcs.intersects')
        self.orm_dao.valid_funcs.append('geo_funcs._within_distance')
        self.orm_dao.expression_locals['geo_funcs'] = geo_funcs

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
                           Column('type', String),
                           Column('type_id', Integer),
                           Column('area', Float),
                           Column('z', Float),
                           Column('habitat_composition', PickleType),
                           GeometryExtensionColumn('geom', MultiPolygon(2)),
                          ),
            'is_spatial': True,
        }
        mappings['Cell']['mapper_kwargs'] = {
            'properties': {
                'geom': GeometryColumn(mappings['Cell']['table'].c.geom)
            }
        }

        # Habitat.
        mappings['Habitat'] = {
            'table': Table('habitat', self.metadata,
                           Column('id', Integer, primary_key=True),
                           Column('substrate_id', String),
                           Column('energy_id', String),
                           Column('z', Float),
                           Column('area', Float),
                           GeometryExtensionColumn('geom', MultiPolygon(2)),
                          ),
            'is_spatial': True,
        }
        mappings['Habitat']['mapper_kwargs'] = {
            'properties': {'geom': GeometryColumn(mappings['Habitat']['table'].c.geom)}
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
                           Column('label', String),
                           Column('description', Text)
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
                           Column('cell_id', Integer),
                           Column('gear_id', String),
                           Column('swept_area', Float),
                           Column('hours_fished', Float),
                           Column('time', Integer),
                          ),
        }

        # Result.
        mappings['Result'] = {
            'table': Table('result', self.metadata,
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
                            Column('projection', String),
                           ),
        }

        for class_name, mapping in mappings.items():
            if mapping.get('is_spatial'):
                GeometryDDL(mapping['table'])
            mapped_class = self.get_local_mapped_class(
                getattr(sasi_models, class_name),
                mapping['table'],
                class_name,
                **mapping.get('mapper_kwargs', {})
            )
            schema['sources'][class_name] = mapped_class

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

    def query(self, query_def, format_='result_cursor'):
        q = self.orm_dao.get_query(query_def)
        if format_ == 'result_cursor':
            return self.orm_dao.get_result_cursor(q)
        elif format_ == 'query_obj':
            return q

    def save_dicts(self, source_id, dicts, batch_insert=True, batch_size=10000,
                   commit=True, logger=logging.getLogger()):

        table = self.orm_dao.get_table_for_class(
            self.schema['sources'][source_id])

        if not batch_insert:
            self.session.execute(table.insert(), dicts)
        else:
            batch = []
            batch_counter = 1
            for d in dicts:
                # If batch is at batch size, process the batch w/out committing.
                if (batch_counter % batch_size) == 0: 
                    self.save_dicts(
                        source_id,
                        batch, 
                        batch_insert=False, 
                        commit=False
                    )
                    batch = []

                    logger.info("%d of %d items (%.1f%%)" % (
                        batch_counter, len(dicts), 1.0 * batch_counter/len(dicts) * 100
                    ))

                batch.append(d)
                batch_counter += 1

            # Save any remaining batch items.
            if batch: 
                self.save_dicts(
                    source_id, 
                    batch, 
                    batch_insert=False, 
                    commit=False,
                )


        # Commit if commit is true.
        if commit: 
            self.session.commit()
