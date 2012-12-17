import csv
import logging


class CSV_Ingestor(object):
    def __init__(self, csv_file=None, mappings={}, logger=logging.getLogger(),
                 limit=None, get_count=False, log_interval=1000):
        self.logger = logger
        self.mappings = mappings
        self.limit = limit
        self.get_count = get_count
        self.log_interval = log_interval

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

    def ingest(self):
        num_records = None

        if self.get_count:
            self.logger.info("Counting records...")
            num_records = 0
            for r in self.reader:
                num_records += 1
                if self.limit and num_records == self.limit:
                    break
            self.csv_file.seek(0)
            self.reader = csv.DictReader(self.csv_file)
            self.logger.info("%s total records" % num_records)

        limit = self.limit or num_records

        counter = 0

        for record in self.reader:
            counter += 1

            if (counter % self.log_interval) == 0:
                log_msg = "%d" % counter
                if limit:
                    log_msg += " of %d (%.1f%%)" % (
                        limit, 1.0 * counter/limit * 100)
                self.logger.info(log_msg)

            target = self.initialize_target_record(counter)

            for mapping in self.mappings:
                raw_value = record.get(mapping['source'])
                if raw_value == None and mapping.get('default'):
                    raw_value = mapping['default']
                processor = mapping.get('processor')
                value = raw_value
                if processor:
                    value = processor(value)
                self.set_target_attr(target, mapping['target'], value)

            self.after_record_mapped(record, target, counter)

            if self.limit is not None and counter == self.limit:
                self.post_ingest(counter)
                return

        self.post_ingest(counter)
        return

    def initialize_target_record(self, counter): 
        pass

    def set_target_attr(self, target, attr, value):
        pass

    def after_record_mapped(self, source_record, target_record, counter): 
        pass

    def post_ingest(self, counter):
        self.csv_file.close()
