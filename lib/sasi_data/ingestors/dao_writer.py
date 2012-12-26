from sasi_data.ingestors.writer import Writer 


class DAOWriter(Writer):
    def __init__(self, dao=None, commit=True, **kwargs):
        Writer.__init__(self, **kwargs)
        self.dao = dao
        self.commit = commit

    def write(self, results, counter, item):
        self.dao.save(item, commit=self.commit)
