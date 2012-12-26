from sasi_data.ingestors.writer import Writer 


class DictWriter(Writer):
    def __init__(self, dict_={}, key_func=lambda r: id(r), **kwargs):
        Writer.__init__(self, **kwargs)
        self.dict_ = dict_
        self.key_func = key_func

    def write(self, results, counter, item):
        key = self.key_func(item)
        self.dict_[key] = item
