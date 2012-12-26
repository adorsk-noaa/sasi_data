from sasi_data.ingestors.processor import Processor


class Writer(Processor):
    def __init__(self, include_keys=None, exclude_keys=['__orig'], **kwargs):
        Processor.__init__(self, **kwargs)
        self.include_keys = include_keys
        self.exclude_keys = exclude_keys

    def process(self, results, counter):
        include_keys = self.include_keys or results.keys()
        for result_key in include_keys:
            if result_key in self.exclude_keys:
                continue
            items = results.get(result_key)
            for item in items:
                self.write(results, counter, item)

    def write(self, results, counter, item):
        pass
