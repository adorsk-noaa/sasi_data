import sasi_data.ingestors as ingestors
import sasi_data.util.gis as gis_util
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
    def __init__(self, dao=None, logger=logging.getLogger(), **kwargs):
        self.dao = dao
        self.logger = logger

    def ingest(self, data_dir=None):

        # CSV to DAO ingests.
        dao_csv_sections = [
            {
                'id': 'substrates',
                'class': self.dao.schema['sources']['Substrate'],
                'mappings': [
                    {'source': 'id', 'target': 'id'}
                ]
            },
            {
                'id': 'energies',
                'class': self.dao.schema['sources']['Energy'],
                'mappings': [
                    {'source': 'id', 'target': 'id'}
                ]
            },
            {
                'id': 'features',
                'class': self.dao.schema['sources']['Feature'],
                'mappings': [
                    {'source': 'id', 'target': 'id'},
                    {'source': 'category', 'target': 'category'}
                ]
            },
            {
                'id': 'gears',
                'class': self.dao.schema['sources']['Gear'],
                'mappings': [
                    {'source': 'id', 'target': 'id'},
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
                     'default': ("+proj=moll +lon_0=0 +x_0=0 +y_0=0 "
                                 "+ellps=WGS84 +datum=WGS84 +units=m "
                                 "+no_defs")
                    }
                ],
            },
        ]

        for section in dao_csv_sections:
            base_msg = "Ingesting '%s'..." % section['id']
            self.logger.info(base_msg)
            csv_file = os.path.join(data_dir, section['id'], 'data',
                                    "%s.csv" % section['id'])
            ingestor = ingestors.DAO_CSV_Ingestor(
                dao=self.dao, 
                csv_file=csv_file, 
                clazz=section['class'],
                mappings=section['mappings'],
                logger=self.get_section_logger(section['id'], base_msg)
            ) 
            ingestor.ingest()

        # Keep a shortcut to the model parameters.
        self.model_parameters = self.dao.query('__ModelParameters').fetchone()

        # Shapefile data.
        shp_sections = [
            {
                'id': 'habitats',
                'class': self.dao.schema['sources']['Habitat'],
                'reproject_to': '+init=epsg:4326',
                'mappings': [
                    {'source': 'SUBSTRATE', 'target': 'substrate_id'},
                    {'source': 'ENERGY', 'target': 'energy_id'},
                    {'source': 'Z', 'target': 'z', 
                     'processor': lambda value: -1.0 * float(value)},
                ]
            },
            {
                'id': 'grid',
                'class': self.dao.schema['sources']['Cell'],
                'reproject_to': '+init=epsg:4326',
                'mappings': [
                    {'source': 'TYPE', 'target': 'type'},
                    {'source': 'TYPE_ID', 'target': 'type_id'},
                ]
            }
        ]
        for section in shp_sections:
            base_msg = "Ingesting '%s'..." % section['id']
            self.logger.info(base_msg)
            shp_file = os.path.join(data_dir, section['id'], 'data',
                                    "%s.shp" % section['id'])
            ingestor = ingestors.Shapefile_Ingestor(
                dao=self.dao,
                shp_file=shp_file,
                clazz=section['class'],
                reproject_to=section.get('reproject_to'),
                mappings=section['mappings'],
                logger=self.get_section_logger(section['id'], base_msg)
            ) 
            ingestor.ingest()

        # Fishing efforts.
        effort_dir = os.path.join(data_dir, 'fishing_efforts')
        effort_model_file = os.path.join(effort_dir, 'model.csv')
        model_info_reader = csv.DictReader(open(effort_model_file, 'rb'))
        model_info = model_info_reader.next()
        self.effort_model_type = model_info.get('model_type')
        if self.effort_model_type == 'realized':
            csv_file = os.path.join(effort_dir, 'data', 'fishing_efforts.csv')
            mappings = []
            for attr in ['cell_id', 'time', 'swept_area', 'gear_id']:
                mappings.append({ 'source': attr, 'target': attr, })
            base_msg = "Ingesting '%s'..." % section['id']
            self.logger.info(base_msg)
            ingestor = ingestors.DAO_CSV_Ingestor(
                dao=self.dao, 
                csv_file=csv_file,
                clazz=self.dao.schema['sources']['Effort'],
                mappings=mappings,
                logger=self.get_section_logger(section['id'], base_msg)
            )
            ingestor.ingest()

        self.post_ingest()

    def get_section_logger(self, section_id, base_msg):
        logger = logging.getLogger("%s_%s" % (id(self), section_id))
        formatter = logging.Formatter(base_msg + ' %(message)s.')
        log_handler = LoggerLogHandler(self.logger)
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        return logger

    def post_ingest(self):
        self.calculate_habitat_areas()
        self.calculate_cell_compositions()

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

        cells = self.dao.query('__Cell').all()
        gears = self.dao.query('__Gear').all()
        total_efforts = len(tsteps) * len(cells) * len(gears)

        counter = 0

        for t in tsteps:
            for cell in self.dao.query('__Cell'):
                for gear in self.dao.query('__Gear'):
                    counter += 1

                    if (counter % log_interval) == 0:
                        logger.info(
                            base_msg + (" generating effort #%d of %d"
                                        " total (%.1f%%)") % (
                                            counter, total_efforts, 
                                            1.0 * counter/total_efforts * 100
                                        )
                        )

                    effort = EffortClass(
                        cell_id=cell.id,
                        time=t,
                        swept_area=cell.area,
                        gear_id=gear.id
                    )
                    self.dao.save(effort, auto_commit=False)
        self.dao.commit()

    def calculate_habitat_areas(self, log_interval=1000):
        base_msg = 'Calculating habitat areas...'
        self.logger.info(base_msg)
        logger = self.get_section_logger('habitat_areas', base_msg)

        habitats = self.dao.query('__Habitat').all()
        num_habitats = len(habitats)
        counter = 0
        for habitat in habitats:
            counter += 1
            if (counter % log_interval) == 0:
                logger.info(
                    base_msg + (" habitat #%d of %d"
                                "total (%.1f%%)") % (
                                    counter, num_habitats, 
                                    1.0 * counter/num_habitats* 100
                                )
                )
            habitat.area = gis_util.get_area(
                str(habitat.geom.geom_wkb), 
                target_proj=str(self.model_parameters.projection)
            )
            self.dao.save(habitat, auto_commit=False)
        self.dao.commit()

    def calculate_cell_compositions(self, log_interval=1000):
        base_msg = 'Calculating cell compositions...'
        self.logger.info(base_msg)
        logger = self.get_section_logger('habitat_areas', base_msg)

        cells = self.dao.query('__Cell').all()
        num_cells = len(cells)
        counter = 0
        for cell in cells:

            counter += 1
            if (counter % log_interval) == 0:
                logger.info(
                    base_msg + (" cell #%d of %d"
                                "total (%.1f%%)") % (
                                    counter, num_cells, 
                                    1.0 * counter/num_cells* 100
                                )
                )

            composition = {}
            cell.z = 0

            # Calculate cell area.
            cell.area = gis_util.get_area(
                str(cell.geom.geom_wkb),
                target_proj=str(self.model_parameters.projection)
            )

            # Calculate habitat composition.
            intersecting_habitats = self.dao.query({
                'SELECT': '__Habitat',
                'WHERE':  [
                    [{'TYPE': 'ENTITY', 
                      'EXPRESSION': ('func.st_intersects(__Habitat__geom,'
                                     '__Cell__geom)')
                     }, '==', True],
                    [{'TYPE': 'ENTITY', 'EXPRESSION': '__Cell__id'}, 
                     '==', cell.id]
                ]
            })

            for habitat in intersecting_habitats:
                intersection = gis_util.get_intersection(
                    str(habitat.geom.geom_wkb),
                    str(cell.geom.geom_wkb),
                )
                intersection_area = gis_util.get_area(
                    str(intersection),
                    target_proj=str(self.model_parameters.projection)
                )
                hab_key = (habitat.substrate_id, habitat.energy_id,)
                pct_area = intersection_area/cell.area
                composition[hab_key] = composition.get(hab_key, 0) + pct_area
                cell.z += pct_area * habitat.z
            cell.habitat_composition = composition

            self.dao.save(cell, auto_commit=False)
        self.dao.commit()
