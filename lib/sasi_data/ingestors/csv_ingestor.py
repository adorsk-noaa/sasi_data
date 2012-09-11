import csv


class CSV_Ingestor(object):
    def __init__(self, csv_file=None, mappings={}):
        self.mappings = mappings
        if isinstance(csv_file, str):
            csv_file = open(csv_file, 'rb')
        self.reader = csv.DictReader(csv_file)
        for i in range(len(self.mappings)):
            if isinstance(self.mappings[i], str):
                self.mappings[i] = {'source': self.mappings[i], 'target':
                                    self.mappings[i]}

    def ingest(self):
        for record in self.reader:
            target = self.initialize_target_record()
            for mapping in self.mappings:
                raw_value = record.get(mapping['source'])
                if raw_value == None and mapping.get('default'):
                    raw_value = mapping['default']
                processor = mapping.get('processor')
                value = raw_value
                if processor:
                    value = processor(value)
                self.set_target_attr(target, mapping['target'], value)
            self.after_record_mapped(record, target)

    def initialize_target_record(self): pass

    def set_target_attr(self, target, attr, value): pass

    def after_record_mapped(self, source_record, target_record): pass
