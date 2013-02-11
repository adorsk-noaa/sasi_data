import csv


class CSVReader(object):
    def __init__(self, csv_file=None, as_unicode=True, encoding='utf-8',
                 **kwargs):
        self.csv_file = csv_file
        self.csv_fh = self.get_csv_fh()
        self.as_unicode = as_unicode
        self.encoding = encoding

    def get_csv_fh(self):
        if isinstance(self.csv_file, str) or isinstance(self.csv_file, unicode):
            return open(self.csv_file, 'rb')
        else:
            return self.csv_file

    def get_records(self):
        for row in csv.DictReader(self.csv_fh):
            if self.as_unicode:
                yield dict([(key, unicode(value, 'utf-8')) for key, value in row.iteritems()])
            else:
                yield row

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
