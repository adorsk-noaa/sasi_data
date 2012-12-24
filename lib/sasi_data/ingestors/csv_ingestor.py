from sasi_data.ingestors.ingestor import Ingestor
import csv
import logging


class CSV_Ingestor(Ingestor):
    def __init__(self, csv_file=None, get_count=False, **kwargs):
        Ingestor.__init__(self, **kwargs)
        self.get_count = get_count

    def prepare_reader(self):
        if isinstance(csv_file, str):
            csv_file = open(csv_file, 'rb')
        self.csv_file = csv_file
        self.reader = csv.DictReader(self.csv_file)

    def pre_ingest(self):
        if self.get_count:
            self.logger.info("Counting records...")
            self.num_records = 0
            for r in self.reader:
                self.num_records += 1
                if self.limit and self.num_records == self.limit:
                    break
            self.csv_file.seek(0)
            self.reader = csv.DictReader(self.csv_file)
            self.logger.info("%s total records" % self.num_records)

    def get_record_attr(self, record, attr):
        raw_value = record.get(attr)
        if raw_value == '':
            raw_value = None
        return raw_value

    def post_ingest(self, counter):
        self.csv_file.close()
