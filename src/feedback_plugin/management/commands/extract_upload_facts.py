from datetime import datetime, timezone

from django.core.management.base import BaseCommand

from feedback_plugin.data_processing import etl
from feedback_plugin.data_processing import extractors


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
        etl.extract_upload_facts(options['start_time'], options['end_time'],
                                 [extractors.ServerVersionExtractor()])
