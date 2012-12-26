import logging


class Ingestor(object):
    def __init__(self, reader=None, processors=[], logger=logging.getLogger(),
                 limit=None, log_interval=1000, **kwargs):
        self.logger = logger
        self.reader = reader
        self.mapper = mapper
        self.limit = limit
        self.log_interval = log_interval

    def ingest(self):
        self.pre_ingest()
        counter = 0
        num_records = self.reader.size
        for record in self.reader.get_records():
            counter += 1
            if (counter % self.log_interval) == 0:
                log_msg = "%d" % counter
                if num_records:
                    log_msg += " of %d (%.1f%%)" % (
                        num_records, 1.0 * counter/num_records* 100)
                self.logger.info(log_msg)

            results = {}
            for processor in self.processors:
                processor.process(record, results, counter)

            if self.limit is not None and counter == self.limit:
                break

        self.reader.close()
