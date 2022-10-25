from django.core.management.base import BaseCommand

from feedback_plugin.data_processing import etl

class Command(BaseCommand):
  def handle(self, *args, **options):
      etl.process_raw_data()
