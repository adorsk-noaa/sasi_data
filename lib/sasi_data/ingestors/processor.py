class Processor(object):
    def __init__(self, key=None, **kwargs):
        if not key:
            key = id(self)
        self.key = key
    def process(record=None, results={}, counter=None):
        pass
