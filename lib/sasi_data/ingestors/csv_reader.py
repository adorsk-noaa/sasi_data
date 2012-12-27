import csv


class CSVReader(object):
    def __init__(self, csv_file=None, **kwargs):
        self.csv_file = csv_file
        self.csv_fh = self.get_csv_fh()

    def get_csv_fh(self):
        if isinstance(self.csv_file, str) or isinstance(self.csv_file, unicode):
            return open(self.csv_file, 'rb')
        else:
            return self.csv_file

    def get_records(self):
        return csv.DictReader(self.csv_fh)

    def get_size(self, limit=None, **kwargs):
        size = 0
        for r in csv.DictReader(self.csv_fh):
            size += 1
            if limit is not None and (size % limit) == 0:
                break
        self.csv_fh.seek(0)
        return size

    def close(self):
        self.csv_fh.close()
