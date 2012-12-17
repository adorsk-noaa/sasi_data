import csv
import logging


class CSV_Ingestor(object):
    def __init__(self, csv_file=None, mappings={}, logger=logging.getLogger(),
                 limit=None):
        self.logger = logger
        self.mappings = mappings
        self.limit = limit
        if isinstance(csv_file, str):
            csv_file = open(csv_file, 'rb')
        self.csv_file = csv_file
        self.reader = csv.DictReader(self.csv_file)
        for i in range(len(self.mappings)):
            if isinstance(self.mappings[i], str):
                self.mappings[i] = {'source': self.mappings[i], 'target':
                                    self.mappings[i]}
            else:
                self.mappings[i].setdefault(
                    'target', self.mappings[i]['source'])

    def ingest(self, log_interval=1000):
        records = [r for r in self.reader]
        num_records = len(records)
        self.logger.info("%s total records" % num_records)
        counter = 0
        limit = self.limit or num_records
        for record in records[:limit]:
            counter += 1
            if (counter % log_interval) == 0:
                self.logger.info(
                    base_msg + ("%d of %d (%.1f%%)" % (
                        counter, num_records, 
                        1.0 * counter/num_records* 100)))

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

        self.csv_file.close()

    def initialize_target_record(self): pass

    def set_target_attr(self, target, attr, value): pass

    def after_record_mapped(self, source_record, target_record): pass
