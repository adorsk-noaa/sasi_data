from sasi_data.ingestors.processor import Processor 


class DictWriter(Processor):
    def __init__(self, dict_={}, key_func=lambda r: id(r), **kwargs):
        Processor.__init__(self, **kwargs)
        self.dict_ = dict_
        self.key_func = key_func

    def process(self, data=None, counter=None, total=None, **kwargs):
        key = self.key_func(data)
        self.dict_[key] = data
