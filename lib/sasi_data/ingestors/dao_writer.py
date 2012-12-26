from sasi_data.ingestors.processor import Processor


class DAOWriter(Processor):
    def __init__(self, dao=None, commit_interval=None, **kwargs):
        Processor.__init__(self, **kwargs)
        self.dao = dao
        self.commit_interval = commit_interval

    def process(self, data=None, counter=None, total=None, **kwargs):
        self.dao.save(data, commit=False)
        if self.commit_interval:
            if (counter % self.commit_interval) == 0 or counter == total:
                self.dao.commit()
