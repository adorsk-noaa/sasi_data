from sasi_data.ingestors.ingestor import Ingestor
import csv
import logging


class CSV_Ingestor(Ingestor):
    def __init__(self, csv_file=None, get_count=False, **kwargs):
        self.csv_file = csv_file
        self.get_count = get_count
        Ingestor.__init__(self, **kwargs)

    def prepare_reader(self):
        if isinstance(self.csv_file, str):
            self.csv_fh = open(self.csv_file, 'rb')
        else:
            self.csv_fh = self.csv_file
        self.reader = csv.DictReader(self.csv_fh)

    def pre_ingest(self):
        if self.get_count:
            self.logger.info("Counting records...")
            self.num_records = 0
            for r in self.reader:
                self.num_records += 1
                if self.limit and self.num_records == self.limit:
                    break
            self.csv_fh.seek(0)
            self.reader = csv.DictReader(self.csv_fh)
            self.logger.info("%s total records" % self.num_records)

    def get_record_attr(self, record, attr):
        raw_value = record.get(attr)
        if raw_value == '':
            raw_value = None
        return raw_value

    def post_ingest(self, counter):
        self.csv_fh.close()
