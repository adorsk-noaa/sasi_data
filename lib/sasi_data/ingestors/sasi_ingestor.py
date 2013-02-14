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

def robust_float(value):
    """ Return None for 'None' or empty """
    if value is None or value == '':
        return None
    else:
        return float(value)

def robust_int(value):
    """ Return None for 'None' or empty """
    float_value = robust_float(value)
    if float_value is None:
        return None
    return int(float_value)

def parse_bool(v):
    if type(v) in [str, unicode]:
        if v.upper() == 'TRUE':
            return True
    try:
        return bool(float(v))
    except:
        return False

class SASI_Ingestor(object):
    def __init__(self, data_dir=None, dao=None, logger=logging.getLogger(),
                 config={}, hash_cell_size=.1, **kwargs):
        self.data_dir = data_dir
        self.dao = dao
        self.logger = logger
        self.hash_cell_size = hash_cell_size
        self.config = config
        self.commit_interval = config.get('commit_interval', 1e4)

    def ingest(self):

        # Define generic CSV ingests.
        csv_sections = [
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
                    {'source': 'generic_id', 'target': 'generic_id'},
                    {'source': 'is_generic', 'target': 'is_generic', 
                     'processor': parse_bool},
                    {'source': 'label', 'target': 'label'},
                    {'source': 'description', 'target': 'description'},
                    {'source': 'min_depth', 'processor': robust_float},
                    {'source': 'max_depth', 'processor': robust_float},
                ]
            },
            {
                'id': 'va',
                'class': self.dao.schema['sources']['VA'],
                'mappings': [
                    {'source': 'gear_id', 'target': 'gear_id'},
                    {'source': 'feature_id', 'target': 'feature_id'},
                    {'source': 'substrate_id', 'target': 'substrate_id'},
                    {'source': 'energy_id', 'target': 'energy_id'},
                    {'source': 's', 'target': 's', 'processor': robust_int},
                    {'source': 'r', 'target': 'r', 'processor': robust_int},
                ]
            },
            {
                'id': 'fishing_efforts',
                'optional': True,
                'class': self.dao.schema['sources']['Effort'],
                'mappings': [
                    {'source': 'cell_id', 'target':'cell_id', 
                     'processor': robust_int},
                    {'source': 'time', 'target': 'time', 
                     'processor': robust_int},
                    'gear_id',
                    # note: we assume a is already in km^2.
                    {'source': 'a', 'processor': robust_float},
                    {'source': 'hours_fished', 'processor': robust_float},
                    {'source': 'value', 'processor': robust_float},
                ]
            },
            {
                'id': 'model_parameters',
                'class': self.dao.schema['sources']['ModelParameters'],
                'mappings': [
                    'time_start',
                    'time_end',
                    'time_step',
                    {'source': 't_0', 'target': 't_0', 
                     'processor': robust_float},
                    {'source': 't_1', 'target': 't_1', 
                     'processor': robust_float},
                    {'source': 't_2', 'target': 't_2', 
                     'processor': robust_float},
                    {'source': 't_3', 'target': 't_3', 
                     'processor': robust_float},
                    {'source': 'w_0', 'target': 'w_0', 
                     'processor': robust_float},
                    {'source': 'w_1', 'target': 'w_1', 
                     'processor': robust_float},
                    {'source': 'w_2', 'target': 'w_2', 
                     'processor': robust_float},
                    {'source': 'w_3', 'target': 'w_3', 
                     'processor': robust_float},
                    {'source': 'effort_model', 'default': 'nominal'},
                    {'source': 'projection', 'target': 'projection',
                     # Use the mollweide projection as the default.
                     'default': gis_util.get_default_geographic_crs(),
                    }
                ],
            },
            ]

        for section in csv_sections:
            self.ingest_csv_section(section)

        # Convenience shortcuts.
        self.model_parameters = self.dao.query('__ModelParameters').fetchone()
        self.geographic_crs = self.model_parameters.projection

        self.ingest_grid()
        self.ingest_habitats()

        self.post_ingest()

    def ingest_csv_section(self, section):
        csv_file = os.path.join(self.data_dir, "%s.csv" % section['id'])
        if not os.path.isfile(csv_file):
            if not section.get('optional'):
                raise Exception(
                    ("Error ingesting '%s': "
                     "File '%s' is required and was not found.") % 
                    (section['id'], csv_file)
                )
            else:
                return

        base_msg = "Ingesting '%s'..." % section['id']
        self.logger.info(base_msg)
        section_config = self.config.get('sections', {}).get(
            section['id'], {})

        Ingestor(
            reader=CSVReader(csv_file=csv_file),
            processors=[
                ClassMapper(clazz=section['class'],
                            mappings=section['mappings']),
                DAOWriter(dao=self.dao, commit_interval=self.commit_interval),
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
        grid_file = os.path.join(self.data_dir, 'grid', "grid.shp")
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
                        {'source': 'ID', 'target': 'id', 'processor': int}, 
                        {'source': '__shape', 'target': 'shape'},
                        {'source': '__shape', 'target': 'geom_wkt',
                         'processor': gis_util.shape_to_wkt}
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
        habs_file = os.path.join(self.data_dir, 'habitats', "habitats.shp")
        habs_config = self.config.get('sections', {}).get('habitats', {})

        def add_to_habs_spatial_hash(data=None, **kwargs):
            self.habs_spatial_hash.add_rect(data.mbr, data)
            return data

        def process_neg_depth(neg_depth):
            if neg_depth is not None:
                depth = -1.0 * float(neg_depth)
                return depth

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
                        {'source': 'Z', 'target': 'depth', 
                         'processor': process_neg_depth},
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

    def get_section_logger(self, section_id, base_msg):
        logger = logging.getLogger("%s_%s" % (id(self), section_id))
        formatter = logging.Formatter(base_msg + ' %(message)s.')
        log_handler = LoggerLogHandler(self.logger)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        logger.setLevel(self.logger.level)
        return logger

    def post_ingest(self):
        self.post_process_cells()

        # Allow for cells and habs to be garbage collected.
        self.cells = None
        self.habs = None
        self.habs_spatial_hash = None

    def post_process_cells(self, log_interval=1000):
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
            cell.depth = 0

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
                cell.depth += pct_area * hab.depth

            cell.habitat_composition = composition

            # Convert cell area to km^2.
            cell.area = cell.area/(1000.0**2)

            self.dao.save(cell, commit=False)
        self.dao.commit()

    # Define processor for adding area, mbr to geom entities.
    def add_area_mbr(self, data=None, **kwargs):
        data.area = gis_util.get_shape_area(
            data.shape, target_crs=self.geographic_crs)
        data.mbr = gis_util.get_shape_mbr(data.shape)
        return data 
