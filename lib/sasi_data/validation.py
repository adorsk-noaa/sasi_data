import os
import csv
import shapefile
import tempfile


shp_extensions = ['shp', 'shx', 'dbf']

class SASIDataValidationError(Exception): pass

class SASIDataValidator(object):

    def __init__(self, config=None):
        self.config = config

    def validate(self):
        """ Validate SASI Data, section by section."""
        sections = [
            'substrates',
            'features',
            'gears',
            'va',
            'habitats',
            'grid',
            'model_parameters',
            'fishing_efforts',
            'map_layers',
        ]

        for section in sections:
            self.validate_section(section)

    def validate_section(self, section):
        validator = self.get_section_validator(self.config, section)
        if validator:
            validator.validate()

    def get_section_validator(self, config=None, section=None):
        validator = None

        # CSV sections.
        if section in [
            'substrates',
            'features',
            'gears',
            'va',
            'model_parameters',
        ] :
            data_file_path = os.path.join(section, 'data', section + '.csv')
            required_file_paths = [data_file_path]

            required_columns = []
            if section == 'substrates':
                required_columns = [
                    'id'
                ]

            elif section == 'features':
                required_columns = [
                    'id', 
                    'category'
                ]

            elif section == 'va':
                required_columns = [
                    "Gear ID", 
                    "Substrate ID",
                    "Feature ID",
                    "Energy",
                    "S",
                    "R"
                ]

            elif section == 'model_parameters':
                required_columns = [
                    "time_start",
                    "time_end",
                    "time_step",
                ]

            validator = CSVFileSectionValidator(
                config=config, 
                section=section, 
                required_file_paths=required_file_paths,
                column_requirements=[{
                    'file_path': data_file_path,
                    'required_columns': required_columns
                }]
            )

        # Shapefile sections.
        elif section in [
            'habitats',
            'grid',
        ] :
            required_file_paths = [
                os.path.join(section, 'data', section) + '.' + extension for
                extension in shp_extensions]
            shp_file_path = required_file_paths[0]

            required_columns = []
            if section == 'habitats':
                required_columns = [
                    'substrate',
                    'z',
                    'energy'
                ]

            elif section == 'grid':
                required_columns = [
                    'type_id',
                    'type',
                ]

            validator = ShpFileSectionValidator(
                config=config, 
                section=section, 
                required_file_paths=required_file_paths,
                column_requirements=[{
                    'file_path': shp_file_path,
                    'required_columns': required_columns
                }]
            )

        # Fishing efforts section.
        elif section == 'fishing_efforts':
            validator = FishingEffortsFileSectionValidator(
                config=config, 
                section=section, 
            )

        # Map Layers section.
        elif section == 'map_layers':
            validator = MapLayersFileSectionValidator(
                config=config, 
                section=section, 
            )

        return validator

class SectionValidator(object):
    def __init__(self, data_dir, section=None):
        self.data_dir = data_dir
        self.section = section
        self.section_dir = os.path.join(data_dir, section['id'])

class FileSectionValidator(SectionValidator):
    def __init__(self, required_file_paths=[], **kwargs):
        SectionValidator.__init__(self, **kwargs)
        self.required_file_paths = required_file_paths

    def validate(self):
        self.validate_required_files()

    def validate_required_files(self):
        for file_path in self.required_file_paths:
            abs_file_path = os.path.join(self.section_dir, file_path)
            if not os.path.isfile(abs_file_path):
                raise SASIDataValidationError("File '%s' was not found. "
                                              " This file is required."
                                              " Names are case-sensitive."
                                              % (file_path)
                                             )


class CSVFileSectionValidator(FileSectionValidator):
    def __init__(self, column_requirements=[], **kwargs):
        FileSectionValidator.__init__(self, **kwargs)
        self.column_requirements = column_requirements

    def validate(self):
        super(CSVFileSectionValidator, self).validate()
        for requirement in self.column_requirements:
            file_path = requirement['file_path']
            abs_file_path os.path.join(self.section_dir, file_path)
            required_columns = requirement['required_columns']
            csv_reader = csv.DictReader(open(abs_file_path, 'rb'))
            csv_columns = csv_reader.fieldnames
            for column in required_columns:
                if column not in csv_columns:
                    raise SASIDataValidationError(
                        "Column '%s' was not found in file %s."
                        " This column is required."
                        " Names are case-sensitive."
                        % (column, file_path)
                    ) 

class ShpFileSectionValidator(FileSectionValidator):
    def __init__(self, column_requirements=[], **kwargs):
        FileSectionValidator.__init__(self, **kwargs)
        self.column_requirements = column_requirements

    def validate(self):
        super(ShpFileSectionValidator, self).validate()
        for requirement in self.column_requirements:
            file_path = requirement['file_path']
            required_columns = requirement['required_columns']
            abs_file_path = os.path.join(self.section_dir, file_path)
            shp_reader = shapefile.Reader(abs_file_path)
            shp_columns = [shp_field[0].upper() 
                           for shp_field in shp_reader.fields]
            for column in required_columns:
                if column.upper() not in shp_columns:
                    raise ValidationError(
                        "Column '%s' was not found in file '%s'." 
                        " This column is required."
                        " Names are *not* case-sensitive for this type of file."
                        % (column, file_path)
                    )

class FishingEffortsFileSectionValidator(FileSectionValidator):
    def __init__(self, **kwargs):
        FileSectionValidator.__init__(self, **kwargs)

    def validate(self):
        super(FishingEffortsFileSectionValidator, self).validate()
        model_config_path = os.path.join(self.section_dir, 'model.csv')
        csv_validator = CSVFileSectionValidator(
            config=self.config,
            section=self.section,
            required_file_paths=[model_config_path],
            column_requirements=[{
                'file_path': model_config_path,
                'required_columns': ['model_type']
            }]
        )
        csv_validator.validate()
        model_config_reader = csv.DictReader(open(model_config_path, 'rb'))
        model_config = model_config_reader.next()
        model_type = model_config.get('model_type')

        if model_type == 'from_data':
            data_file_path = os.path.join(self.section, 'data',
                                     'fishing_efforts.csv')
            data_validator = CSVFileSectionValidator(
                config=self.config,
                section=self.section,
                required_file_paths=[data_file_path],
                column_requirements=[{
                    'file_path': data_file_path,
                    'required_columns': [
                        #@TODO: FILL THIS IN.
                    ]
                }]
            )
            data_validator.validate()

        elif not model_type == 'uniform_density':
            raise SASIDataValidationError(
                "Invalid fishing effort model type '%s', in file"
                " '%s', in archive file '%s'."
                " Model type must be 'from_data', or 'uniform_density'."
                " Names are case-sensitive."
            % (model_type, model_config_path, self.section_file.filename)) 


class MapLayersFileSectionValidator(FileSectionValidator):
    def __init__(self, **kwargs):
        FileSectionValidator.__init__(self, **kwargs)

    def validate(self):
        super(MapLayersFileSectionValidator, self).validate()
        map_layers = []
        data_base_path = os.path.join(self.section_dir, "data")
        for item in os.listdir(data_base_path):
            item_path = os.path.join(data_base_path, item)
            if os.path.isdir(item_path):
                map_layers.append({
                    'name': item,
                    'path': item_path
                })

        for map_layer in map_layers:
            required_file_paths = []
            for extension in shp_extensions:
                required_file_paths.append(os.path.join(self.section, 
                    'data', map_layer['name'], 
                    map_layer['name'] + '.' + extension))

            layer_validator = FileSectionValidator(
                config=self.config,
                section=self.section,
                required_file_paths=required_file_paths
            )
            layer_validator.validate()
