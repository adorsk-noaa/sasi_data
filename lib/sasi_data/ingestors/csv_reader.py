import csv


class CSV_Reader(object):
    def __init__(self, csv_file=None, **kwargs):
        self.csv_file = csv_file
        self.get_count = get_count
        self.csv_fh = self.get_csv_fh()

    def get_csv_fh(self):
        if isinstance(self.csv_file, str):
            return open(self.csv_file, 'rb')
        else:
            return self.csv_file

    def get_records(self):
        return csv.DictReader(self.csv_fh())

    @property
    def size(self):
        if not hasattr(self, '_size'):
            self._size = len([r for r in self.csv.reader(self.get_csv_fh())])
        return self._size

    def close(self):
        self.csv_fh.close()
