from ...data_processing import etl, extractors
from ._parallel_fact_extractor import ProcessPoolFactExtractor


class Command(ProcessPoolFactExtractor):
    def __init__(self, *args, **kwargs):
        super().__init__(etl.extract_upload_facts,
                         [extractors.AllUploadFactExtractor()],
                         *args, **kwargs)
