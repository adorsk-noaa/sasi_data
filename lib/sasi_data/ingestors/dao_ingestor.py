from sasi_data.ingestors.ingestor import Ingestor

class DAO_Ingestor(Ingestor):
    """ This is intended to be a mixin w/ Ingestor. """
    def __init__(self, dao=None, commit_interval=1e3, **kwargs):
        self.dao = dao
        self.commit_interval = commit_interval

    def post_record_mapped(self, record, target, counter):
        self.dao.save(target, commit=False)
        if self.commit_interval and (counter % self.commit_interval) == 0:
            self.dao.commit()

    def post_ingest(self, counter):
        if self.commit_interval:
            self.dao.commit()
