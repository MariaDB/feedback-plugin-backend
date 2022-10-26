from datetime import datetime, timezone

from django.core.management.base import BaseCommand

from feedback_plugin.data_processing import etl
from feedback_plugin.data_processing import extractors

class Command(BaseCommand):

  @staticmethod
  def date_as_string_with_timezone(string):
    date = datetime.strptime(string, '%Y-%m-%d')
    date = date.replace(tzinfo=timezone.utc)
    return date


  def add_arguments(self, parser):
    parser.add_argument('start_time', type=Command.date_as_string_with_timezone)
    parser.add_argument('end_time', type=Command.date_as_string_with_timezone)

  def handle(self, *args, **options):
    etl.extract_server_facts(options['start_time'], options['end_time'], [extractors.ArchitectureExtractor()])
