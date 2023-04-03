from concurrent.futures import ProcessPoolExecutor, wait
from datetime import datetime, timedelta, timezone

from django.core.management.base import BaseCommand

from feedback_plugin.data_processing import etl, extractors


class Command(BaseCommand):
    @staticmethod
    def date_with_tz_from_str(string: str) -> datetime:
        date = datetime.strptime(string, '%Y-%m-%d')
        date = date.replace(tzinfo=timezone.utc)
        return date

    def add_arguments(self, parser):
        parser.add_argument('start_time', type=Command.date_with_tz_from_str)
        parser.add_argument('end_time', type=Command.date_with_tz_from_str)
        parser.add_argument('--workers', type=int, default=4,
                            help='How many threads to use to compute server '
                                 'facts')

    def handle(self, *args, **options):
        start_time = options['start_time']
        end_time = options['end_time']
        workers = options['workers']

        with ProcessPoolExecutor(max_workers=workers) as executor:
            jobs = []
            while start_time + timedelta(seconds=60 * 60 * 24) <= end_time:
                cur_end = start_time + timedelta(seconds=60 * 60 * 24)

                job = executor.submit(etl.extract_server_facts,
                                      start_time, cur_end,
                                      [extractors.AllServerFactExtractor()],
                                      end_inclusive=False)
                jobs.append(job)
                start_time = cur_end

            wait(jobs)

            etl.extract_server_facts(start_time, end_time,
                                     [extractors.AllServerFactExtrator()],
                                     end_inclusive=True)
