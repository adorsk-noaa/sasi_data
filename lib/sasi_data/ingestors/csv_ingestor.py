import csv
import logging


class CSV_Ingestor(object):
    def __init__(self, csv_file=None, mappings={}, logger=logging.getLogger()):
        self.logger = logger
        self.mappings = mappings
        if isinstance(csv_file, str):
            csv_file = open(csv_file, 'rb')
        self.reader = csv.DictReader(csv_file)
        for i in range(len(self.mappings)):
            if isinstance(self.mappings[i], str):
                self.mappings[i] = {'source': self.mappings[i], 'target':
                                    self.mappings[i]}

    def ingest(self, log_interval=1000):
        records = [r for r in self.reader]
        total_records = len(records)
        counter = 0
        for record in records:
            counter += 1
            if (counter % log_interval) == 0:
                self.logger.info(
                    base_msg + ("ingesting record #%d of %d"
                                "total (%.1f%%)") % (
                                    counter, total_records, 
                                    1.0 * counter/total_records* 100
                                )
                )
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
