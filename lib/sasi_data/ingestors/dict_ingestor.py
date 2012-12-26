from sasi_data.ingestors.ingestor import Ingestor

class Dict_Ingestor(Ingestor):
    """ This is intended to be a mixin w/ Ingestor. """
    def __init__(self, dict_={}, key_func=lambda t: t.id, **kwargs):
        self.dict = dict_
        self.key_func = key_func
    def post_record_mapped(self, record, target, counter):
        key = self.key_func(target)
        self.dict[key] = target
