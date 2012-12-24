from sasi_data.ingestors.shapefile_ingestor import Shapefile_Ingestor
from sasi_data.ingestors.dao_ingestor import DAO_Ingestor

class DAO_Shapefile_Ingestor(Shapefile_Ingestor, DAO_Ingestor):
    def __init__(self, **kwargs):
        Shapefile_Ingestor.__init__(self, **kwargs)
        DAO_Ingestor.__init__(self, **kwargs)

    def post_record_mapped(self, *args, **kwargs)
        Shapefile_Ingestor.post_record_mapped(self, *args, **kwargs)
        DAOIngestor.post_record_mapped(self, *args, **kwargs)

    def post_ingest(self, *args, **kwargs):
        Shapefile_Ingestor.post_ingest(self, *args, **kwargs)
        DAOIngestor.post_ingest(self, *args, **kwargs)
