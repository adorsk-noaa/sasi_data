from sasi_data.ingestors.ingestor import Ingestor
from sasi_data.ingestors.csv_reader import CSVReader
from sasi_data.ingestors.shapefile_reader import ShapefileReader
from sasi_data.ingestors.dao_writer import DAOWriter 
from sasi_data.ingestors.dict_writer import DictWriter 
from sasi_data.ingestors.mapper import ClassMapper
import sasi_data.util.gis as gis_util
from sasi_data.util.spatial_hash import SpatialHash
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import literal_column
import os
import csv
import logging


class LoggerLogHandler(logging.Handler):
    """ Custom log handler that logs messages to another
    logger. This can be used to chain together loggers. """
    def __init__(self, logger=None, **kwargs):
        logging.Handler.__init__(self, **kwargs)
        self.logger = logger
    def emit(self, record):
        self.logger.log(record.levelno, self.format(record))

class SASI_Ingestor(object):
    def __init__(self, data_dir=None, dao=None, logger=logging.getLogger(),
                 config={}, hash_cell_size=.1, **kwargs):
        self.data_dir = data_dir
        self.dao = dao
        self.logger = logger
        self.hash_cell_size = hash_cell_size
        self.config = config

    def ingest(self):

        # Define generic CSV ingests.
        dao_csv_sections = [
            {
                'id': 'substrates',
                'class': self.dao.schema['sources']['Substrate'],
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                    {'source': 'label', 'target': 'label'},
                    {'source': 'description', 'target': 'description'},
                ]
            },
            {
                'id': 'energies',
                'class': self.dao.schema['sources']['Energy'],
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                    {'source': 'label', 'target': 'label'},
                    {'source': 'description', 'target': 'description'},
                ]
            },
            {
                'id': 'feature_categories',
                'class': self.dao.schema['sources']['FeatureCategory'],
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                    {'source': 'label', 'target': 'label'},
                    {'source': 'description', 'target': 'description'},
                ]
            },
            {
                'id': 'features',
                'class': self.dao.schema['sources']['Feature'],
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                    {'source': 'category', 'target': 'category'},
                    {'source': 'label', 'target': 'label'},
                    {'source': 'description', 'target': 'description'},
                ]
            },
            {
                'id': 'gears',
                'class': self.dao.schema['sources']['Gear'],
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                    {'source': 'label', 'target': 'label'},
                    {'source': 'description', 'target': 'description'},
                ]
            },
            {
                'id': 'va',
                'class': self.dao.schema['sources']['VA'],
                'mappings': [
                    {'source': 'Gear ID', 'target': 'gear_id'},
                    {'source': 'Feature ID', 'target': 'feature_id'},
                    {'source': 'Substrate ID', 'target': 'substrate_id'},
                    {'source': 'Energy', 'target': 'energy_id'},
                    {'source': 'S', 'target': 's'},
                    {'source': 'R', 'target': 'r'},
                ]
            },
            {
                'id': 'model_parameters',
                'class': self.dao.schema['sources']['ModelParameters'],
                'mappings': [
                    {'source': 'time_start', 'target': 'time_start'},
                    {'source': 'time_end', 'target': 'time_end'},
                    {'source': 'time_step', 'target': 'time_step'},
                    {'source': 't_0', 'target': 't_0'},
                    {'source': 't_1', 'target': 't_1'},
                    {'source': 't_2', 'target': 't_2'},
                    {'source': 't_3', 'target': 't_3'},
                    {'source': 'w_0', 'target': 'w_0'},
                    {'source': 'w_1', 'target': 'w_1'},
                    {'source': 'w_2', 'target': 'w_2'},
                    {'source': 'w_3', 'target': 'w_3'},
                    {'source': 'projection', 'target': 'projection',
                     # Use the mollweide projection as the default.
                     'default': gis_util.get_default_geographic_crs(),
                    }
                ],
            },
        ]

        for section in dao_csv_sections:
            self.ingest_csv_section(section)

        # Convenience shortcuts.
        self.model_parameters = self.dao.query('__ModelParameters').fetchone()
        self.geographic_crs = self.model_parameters.projection

        self.ingest_grid()
        self.ingest_habitats()
        self.ingest_efforts()

        self.post_ingest()

    def ingest_csv_section(self, section):
        base_msg = "Ingesting '%s'..." % section['id']
        self.logger.info(base_msg)
        csv_file = os.path.join(self.data_dir, section['id'], 'data',
                                "%s.csv" % section['id'])

        section_config = self.config.get('sections', {}).get(
            section['id'], {})

        Ingestor(
            reader=CSVReader(csv_file=csv_file),
            processors=[
                ClassMapper(clazz=section['class'],
                            mappings=section['mappings']),
                DAOWriter(dao=self.dao)
            ],
            logger=self.get_section_logger(section['id'], base_msg),
            limit=section_config.get('limit'),
        ).ingest()
        self.dao.commit()

    def ingest_grid(self):
        base_msg = "Ingesting 'grid'..."
        self.logger.info(base_msg)
        grid_logger = self.get_section_logger('grid', base_msg)

        self.cells = {}
        grid_file = os.path.join(self.data_dir, 'grid', 'data', "grid.shp")
        grid_config = self.config.get('sections', {}).get('grid', {})

        Ingestor(
            reader=ShapefileReader(
                shp_file=grid_file,
                reproject_to='EPSG:4326',
            ),
            processors=[
                ClassMapper(
                    clazz=self.dao.schema['sources']['Cell'],
                    mappings=[
                        {'source': 'ID', 'target': 'id'}, 
                        {'source': '__shape', 'target': 'shape'}
                    ]
                ),
                self.add_area_mbr,
                DictWriter(dict_=self.cells),
            ],
            logger=grid_logger,
            limit=grid_config.get('limit'),
        ).ingest()

    def ingest_habitats(self):
        base_msg = "Ingesting 'habitats'..."
        self.logger.info(base_msg)
        habs_logger=self.get_section_logger('habs', base_msg)

        self.habs = {}
        self.habs_spatial_hash = SpatialHash(cell_size=self.hash_cell_size)
        habs_file = os.path.join(self.data_dir, 'habitats', 'data', "habitats.shp")
        habs_config = self.config.get('sections', {}).get('habitats', {})

        def add_to_habs_spatial_hash(data=None, **kwargs):
            self.habs_spatial_hash.add_rect(data.mbr, data)
            return data

        def process_z(z):
            if z is not None:
                z = -1.0 * float(z)
            return z

        Ingestor(
            reader=ShapefileReader(
                shp_file=habs_file,
                reproject_to='EPSG:4326',
            ),
            processors=[
                ClassMapper(
                    clazz=self.dao.schema['sources']['Habitat'],
                    mappings=[
                        {'source': 'SUBSTRATE', 'target': 'substrate_id'},
                        {'source': 'ENERGY', 'target': 'energy_id'},
                        {'source': 'Z', 'target': 'z', 'processor': process_z},
                        {'source': '__shape', 'target': 'shape'}, 
                    ]
                ),
                self.add_area_mbr,
                add_to_habs_spatial_hash,
                DictWriter(dict_=self.habs),
            ],
            logger=habs_logger,
            limit=habs_config.get('limit'),
        ).ingest()

    def ingest_efforts(self):
        effort_dir = os.path.join(self.data_dir, 'fishing_efforts')
        effort_model_file = os.path.join(effort_dir, 'model.csv')
        model_info_reader = csv.DictReader(open(effort_model_file, 'rb'))
        model_info = model_info_reader.next()
        self.effort_model_type = model_info.get('model_type')

        if self.effort_model_type == 'realized':
            section = {
                'id': 'fishing_efforts',
                'class': self.dao.schema['sources']['Effort'],
                'mappings': [
                    'cell_id', 
                    'time', 
                    'swept_area', 
                    'gear_id'
                ]
            }
            self.ingest_csv_section(section)

    def get_section_logger(self, section_id, base_msg):
        logger = logging.getLogger("%s_%s" % (id(self), section_id))
        formatter = logging.Formatter(base_msg + ' %(message)s.')
        log_handler = LoggerLogHandler(self.logger)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        logger.setLevel(self.logger.level)
        return logger

    def post_ingest(self):
        # Calculate cell compositions and save cells to DAO.
        self.calculate_cell_compositions()
        for cell in self.cells.values():
            self.dao.save(cell, commit=False)
        self.dao.commit()

        # Generate nominal efforts if effort_model is 'nominal'.
        if self.effort_model_type == 'nominal':
            self.generate_nominal_efforts()

    def generate_nominal_efforts(self, log_interval=1000):
        base_msg = 'Generating nominal efforts...'
        self.logger.info(base_msg)
        logger = self.get_section_logger('nominal_efforts', base_msg)

        EffortClass = self.dao.schema['sources']['Effort']

        tsteps = [tstep for tstep in range(
            self.model_parameters.time_start,
            self.model_parameters.time_end,
            self.model_parameters.time_step,
        )]

        cells = self.dao.query('__Cell', format_='query_obj')
        gears = self.dao.query('__Gear', format_='query_obj')
        num_gears = gears.count()
        total_efforts = len(tsteps) * cells.count() * gears.count()

        counter = 0

        for t in tsteps:
            for cell in self.dao.query('__Cell'):
                for gear in self.dao.query('__Gear'):
                    counter += 1

                    if (counter % log_interval) == 0:
                        logger.info(" %d of %d (%.1f%%)" % (
                            counter, total_efforts, 
                            1.0 * counter/total_efforts * 100))

                    effort = EffortClass(
                        cell_id=cell.id,
                        time=t,
                        a=cell.area/num_gears,
                        gear_id=gear.id
                    )
                    self.dao.save(effort, commit=False)
        logger.info(" Saving efforts...")
        self.dao.commit()

    def calculate_cell_compositions(self, log_interval=1000):
        base_msg = 'Calculating cell compositions...'
        self.logger.info(base_msg)
        logger = self.get_section_logger('habitat_areas', base_msg)

        num_cells = len(self.cells)
        counter = 0
        for cell in self.cells.values():
            counter += 1
            if (counter % log_interval) == 0:
                logger.info(" %d of %d (%.1f%%)" % (
                    counter, num_cells, 1.0 * counter/num_cells* 100))

            composition = {}
            cell.z = 0

            # Get candidate intersecting habitats.
            candidate_habs = self.habs_spatial_hash.items_for_rect(cell.mbr)
            for hab in candidate_habs:
                intersection = gis_util.get_intersection(cell.shape, hab.shape)
                if not intersection:
                    continue
                intersection_area = gis_util.get_shape_area(
                    intersection,
                    target_crs=self.geographic_crs,
                )
                hab_key = (hab.substrate_id, hab.energy_id,)
                pct_area = intersection_area/cell.area
                composition[hab_key] = composition.get(hab_key, 0) + pct_area
                cell.z += pct_area * hab.z
            cell.habitat_composition = composition

            self.dao.save(cell, commit=False)
        self.dao.commit()

    # Define processor for adding area, mbr to geom entities.
    def add_area_mbr(self, data=None, **kwargs):
        data.area = gis_util.get_shape_area(
            data.shape, target_crs=self.geographic_crs)
        data.mbr = gis_util.get_shape_mbr(data.shape)
        return data 
