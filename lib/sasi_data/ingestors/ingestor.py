import logging


class Ingestor(object):
    def __init__(self, reader=None, processors=[], logger=logging.getLogger(),
                 limit=None, log_interval=1000, **kwargs):
        self.logger = logger
        self.reader = reader
        self.processors = processors
        self.limit = limit
        self.log_interval = log_interval

    def ingest(self):
        counter = 0
        self.logger.info("Counting total number of records...")
        num_records = self.reader.get_size(limit=self.limit)
        self.logger.info("%s total records." % num_records)
        if self.limit is not None:
            self.logger.info("Limiting to %s records" % self.limit)
            num_records = min(num_records, self.limit)
        for record in self.reader.get_records():
            counter += 1
            if (counter % self.log_interval) == 0:
                log_msg = "%d" % counter
                if num_records:
                    log_msg += " of %d (%.1f%%)" % (
                        num_records, 1.0 * counter/num_records* 100)
                self.logger.info(log_msg)

            # Send record through processor chain,
            # passing previous result to next item in the chain.
            data = record
            for processor in self.processors:
                # Processor can be processor obj, or function.
                if hasattr(processor, 'process'):
                    data = processor.process(data=data, counter=counter,
                                             total=num_records)
                else:
                    data = processor(data=data, counter=counter, 
                                     total=num_records)
            data = None

            if counter >= num_records:
                break

        self.reader.close()
