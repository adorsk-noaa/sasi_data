from sasi_data.ingestors.processor import Processor


class Mapper(Processor):
    def __init__(self, mappings=[], **kwargs):
        Processor.__init__(self, **kwargs)
        self.key = key
        self.mappings = self.prepare_mappings(mappings)

    def prepare_mappings(self, mappings=[]):
        prepared_mappings = []
        for mapping in mappings:
            if isinstance(mapping, str):
                prepared_mapping = {'source': mapping, 'target': mapping}
            else:
                prepared_mapping = {}
                prepared_mapping.update(mapping)
                prepared_mapping.setdefault( 'target', mapping['source'])
            prepared_mappings.append(prepared_mapping)
        return prepared_mappings

    def process(self, record, results={}, counter):
        target = self.initialize_target()
        for mapping in self.mappings:
            raw_value = record.get(mapping['source'])
            if raw_value == None and mapping.get('default'):
                raw_value = mapping['default']
            processor = mapping.get('processor')
            value = raw_value
            if processor:
                value = processor(value)
            self.set_target_attr(target, mapping['target'], value)
        results[self.key] = target

    def initialize_target(self, record, counter):
        pass
    def set_target_attr(target, attr, value):
        pass

class ClassMapper(Mapper):
    def __init__(self, clazz=None, **kwargs):
        Mappper.__init__(self, **kwargs)
        self.clazz = clazz
    def initialize_target(self, record, counter):
        return self.clazz()
    def set_target_attr(target,attr, value):
        setattr(target, attr, value)

class DictMapper(Mapper):
    def initialize_target(self, record, counter):
        return {}
    def set_target_attr(target,attr, value):
        target.set(attr, value)
