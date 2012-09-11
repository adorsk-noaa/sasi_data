from sasi_data.ingestors.csv_ingestor import CSV_Ingestor

class DAO_CSV_Ingestor(CSV_Ingestor):
    def __init__(self,dao=None, clazz=None, auto_save=True, **kwargs):
        self.dao = dao
        self.clazz = clazz
        self.auto_save = auto_save
        CSV_Ingestor.__init__(self, **kwargs)

    def initialize_target_record(self):
        return self.clazz()

    def set_target_attr(self, target, attr, value):
        setattr(target, attr, value)

    def after_record_mapped(self, source_record, target_record):
        if self.auto_save:
            self.dao.save(target_record)
