import logging


class Ingestor(object):
    def __init__(self, mappings={}, logger=logging.getLogger(),
                 limit=None, count=None, log_interval=1000, clazz=None, 
                 **kwargs):
        self.logger = logger
        self.mappings = mappings
        self.limit = limit
        self.log_interval = log_interval
        self.num_records = None
        self.clazz = clazz

        self.prepare_mappings()
        self.prepare_records()

    def prepare_mappings(self):
        for i in range(len(self.mappings)):
            if isinstance(self.mappings[i], str):
                self.mappings[i] = {'source': self.mappings[i], 'target':
                                    self.mappings[i]}
            else:
                self.mappings[i].setdefault(
                    'target', self.mappings[i]['source'])

    def prepare_records(self):
        pass

    def pre_ingest(self):
        pass

    def map_record(self, record, target, counter):
        for mapping in self.mappings:
            raw_value = self.get_record_attr(record, mapping['source'])
            if raw_value == None and mapping.get('default'):
                raw_value = mapping['default']
            processor = mapping.get('processor')
            value = raw_value
            if processor:
                value = processor(value)
            self.set_target_attr(target, mapping['target'], value)

    def ingest(self):
        self.pre_ingest()
        self.logger.info("%s total records" % self.num_records)
        total = self.num_records
        if self.limit is not None:
            self.logger.info("limiting to %s records" % self.limit)
            total = self.limit
        counter = 0
        for record in self.records:
            counter += 1
            if (counter % self.log_interval) == 0:
                log_msg = "%d" % counter
                if total:
                    log_msg += " of %d (%.1f%%)" % (
                        total, 1.0 * counter/total * 100)
                self.logger.info(log_msg)

            target = self.initialize_target(counter)
            self.map_record(record, target, counter)
            self.post_record_mapped(record, target, counter)

            if self.limit is not None and counter == self.limit:
                break

        self.post_ingest(counter)
        return

    def get_record_attr(self):
        pass

    def initialize_target(self, counter): 
        return self.clazz()

    def set_target_attr(self, target, attr, value):
        setattr(target, attr, value)

    def post_record_mapped(self, source_record, target_record, counter): 
        pass

    def post_ingest(self, counter):
        pass
