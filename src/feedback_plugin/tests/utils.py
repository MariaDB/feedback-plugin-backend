import csv
from datetime import datetime, timezone
import glob
import os
from pathlib import Path

import yaml

from feedback_plugin.models import RawData
from feedback_plugin.data_processing import etl
from feedback_plugin.data_processing.extractors import AllServerFactExtractor
from feedback_plugin.data_processing.extractors import AllUploadFactExtractor

def load_test_data(test_data_path):
    with open(os.path.join(test_data_path, 'metadata.yml'), 'r') as f:
        metadata = yaml.safe_load(f)

    result = []
    for file in metadata['uploads']:
        time = None
        with open(os.path.join(test_data_path, file['path']), 'r') as f:
            data = f.read()
            reader = csv.reader(data.split('\n'), delimiter='\t')

            for row in reader:
                # "Now" entry tells us when the upload took place.
                if 'Now' in row[0]:
                    time = datetime.fromtimestamp(int(row[1]), tz=timezone.utc)
                    break

        assert(time is not None)

        entry = {
            'country': file['country'],
            'data': bytes(data, encoding='utf8'),
            'time': time,
        }
        result.append(entry)
    return result

def create_test_database(test_data_path=os.path.join(Path(__file__).parent,
                                                     'test_data/')):

    test_data = load_test_data(test_data_path)
    for upload in test_data:
        d = RawData(country=upload['country'],
                    data=upload['data'],
                    upload_time=upload['time'])
        d.save()
    etl.process_raw_data()

    #TODO(cvicentiu) process_raw_data should return these 2 values, based on what
    # it processed.
    start_date = datetime(year=2021, month=1, day=1, tzinfo=timezone.utc)
    end_date = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)

    etl.extract_server_facts(start_date, end_date, [AllServerFactExtractor()])
    etl.extract_upload_facts(start_date, end_date, [AllUploadFactExtractor()])

