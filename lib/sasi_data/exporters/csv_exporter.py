import csv


class CSV_Exporter(object):
    def __init__(self, csv_file=None, objects=[], mappings={}):
        self.mappings = mappings
        self.objects = objects
        if isinstance(csv_file, str):
            csv_file = open(csv_file, 'wb')
        self.writer = csv.writer(csv_file)

        for i in range(len(self.mappings)):
            if isinstance(self.mappings[i], str):
                self.mappings[i] = {'source': self.mappings[i], 'target':
                                    self.mappings[i]}

    def export(self):
        fields = [mapping['target'] for mapping in self.mappings]
        self.writer.writerow(fields)
        for obj in self.objects:
            row = []
            for mapping in self.mappings:
                raw_value = getattr(obj, mapping['source'], None)
                if raw_value == None and mapping.get('default'):
                    raw_value = mapping['default']
                processor = mapping.get('processor')
                if not processor:
                    processor = lambda value: value
                value = processor(raw_value)
                row.append(value)
            self.writer.writerow(row)
