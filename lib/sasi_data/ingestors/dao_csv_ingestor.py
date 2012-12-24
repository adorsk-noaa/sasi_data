from sasi_data.ingestors.csv_ingestor import CSV_Ingestor
from sasi_data.ingestors.dao_ingestor import DAO_Ingestor

class DAO_CSV_Ingestor(CSV_Ingestor, DAO_Ingestor):
    def __init__(self, **kwargs):
        CSV_Ingestor.__init__(self, **kwargs)
        DAO_Ingestor.__init__(self, **kwargs)

    def post_record_mapped(self, *args, **kwargs):
        CSV_Ingestor.post_record_mapped(self, *args, **kwargs)
        DAO_Ingestor.post_record_mapped(self, *args, **kwargs)

    def post_ingest(self, *args, **kwargs):
        CSV_Ingestor.post_ingest(self, *args, **kwargs)
        DAO_Ingestor.post_ingest(self, *args, **kwargs)
