from sasi_data.ingestors.csv_ingestor import CSV_Ingestor

class DAO_CSV_Ingestor(CSV_Ingestor):
    def __init__(self,dao=None, clazz=None, commit_interval=1000, **kwargs):
        self.dao = dao
        self.clazz = clazz
        self.commit_interval = commit_interval
        CSV_Ingestor.__init__(self, **kwargs)

    def initialize_target_record(self, counter):
        return self.clazz()

    def set_target_attr(self, target, attr, value):
        setattr(target, attr, value)

    def after_record_mapped(self, source_record, target_record, counter):
        self.dao.save(target_record, commit=False)
        if self.commit_interval and (counter % self.commit_interval) == 0:
            self.dao.commit()

    def post_ingest(self, counter):
        if self.commit_interval:
            self.dao.commit()
