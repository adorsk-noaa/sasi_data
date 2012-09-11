from sasi_data.ingestors.csv_ingestor import CSV_Ingestor

class Dict_CSV_Ingestor(CSV_Ingestor):
    def __init__(self,**kwargs):
        self.records = []
        CSV_Ingestor.__init__(self, **kwargs)

    def initialize_target_record(self):
        return {}

    def set_target_attr(self, target, attr, value):
        target[attr] = value

    def after_record_mapped(self, source_record, target_record):
        self.records.append(target_record)
