from datetime import datetime, timedelta, timezone

from django.core.management.base import BaseCommand

from ...data_processing import etl, extractors


class Command(BaseCommand):
    @staticmethod
    def date_with_tz_from_str(string: str) -> datetime:
        date = datetime.strptime(string, '%Y-%m-%d')
        date = date.replace(tzinfo=timezone.utc)
        return date

    def add_arguments(self, parser):
        parser.add_argument('start_time', type=Command.date_with_tz_from_str)
        parser.add_argument('end_time', type=Command.date_with_tz_from_str)

    def handle(self, *args, **options):
        start_time = options['start_time']
        end_time = options['end_time']

        while start_time + timedelta(seconds=60 * 60 * 24) <= end_time:
            cur_end = start_time + timedelta(seconds=60 * 60 * 24)

            etl.extract_upload_facts(start_time, cur_end,
                                     [extractors.AllUploadFactExtractor()])
            start_time = cur_end

        etl.extract_upload_facts(start_time, end_time,
                                 [extractors.AllUploadFactExtractor()],
                                 end_inclusive=True)

