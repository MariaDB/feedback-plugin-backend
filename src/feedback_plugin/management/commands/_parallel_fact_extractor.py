from concurrent.futures import ProcessPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from typing import Callable, TypeVar, Generic

from django.core.management.base import BaseCommand

from ...data_processing import extractors


Extractor = TypeVar('Extractor', bound=extractors.DataExtractor)
class ProcessPoolFactExtractor(Generic[Extractor], BaseCommand):
    def __init__(
            self,
            extract_cb: Callable[[datetime, datetime,
                                  list[Extractor], bool], None],
            extractors: list[Extractor],
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._extract_cb = extract_cb
        self._extractors = extractors

    @staticmethod
    def date_with_tz_from_str(string: str) -> datetime:
        date = datetime.strptime(string, '%Y-%m-%d')
        date = date.replace(tzinfo=timezone.utc)
        return date

    def add_arguments(self, parser):
        parser.add_argument(
            'start_time', type=ProcessPoolFactExtractor.date_with_tz_from_str)
        parser.add_argument(
            'end_time', type=ProcessPoolFactExtractor.date_with_tz_from_str)
        parser.add_argument(
            '--workers', type=int, default=4,
            help='How many threads to use to compute facts')

    def handle(self, *args, **options):
        start_time = options['start_time']
        end_time = options['end_time']
        workers = options['workers']

        with ProcessPoolExecutor(max_workers=workers) as executor:
            jobs = []
            while start_time + timedelta(seconds=60 * 60 * 24) <= end_time:
                cur_end = start_time + timedelta(seconds=60 * 60 * 24)

                job = executor.submit(self._extract_cb,
                                      start_time, cur_end,
                                      self._extractors,
                                      end_inclusive=False)
                jobs.append(job)
                start_time = cur_end

            wait(jobs)

            self._extract_cb(start_time, end_time,
                             self._extractors,
                             end_inclusive=True)
