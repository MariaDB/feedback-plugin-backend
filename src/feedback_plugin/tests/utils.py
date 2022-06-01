import csv
from datetime import datetime, timezone
import glob
import os

import yaml

from feedback_plugin.models import RawData
from feedback_plugin.data_processing.etl import process_raw_data

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

def create_test_database(test_data):
    for upload in test_data:
        d = RawData(country=upload['country'],
                    data=upload['data'],
                    upload_time=upload['time'])
        d.save()
    process_raw_data()

