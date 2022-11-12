import copy
import sys

from django.core.management.base import BaseCommand, CommandError

from feedback_plugin.models import (Chart, ChartMetadata, Upload)
from feedback_plugin.data_processing import charts

from sql_utils.utils import print_sql

class DatabaseHasNoUploads(Exception):
    pass

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--recreate', action='store_true')

    @staticmethod
    def get_computation_object(chart_id : str, force_recreate : bool):

        first_upload = Upload.objects.all().order_by('upload_time')[:1]
        last_upload = Upload.objects.all().order_by('-upload_time')[:1]

        try:
            end_date = last_upload.get().upload_time
        except Upload.DoesNotExist:
            raise DatabaseHasNoUploads()

        try:
            chart = Chart.objects.select_related('metadata').get(id=chart_id)
        except Chart.DoesNotExist:
            chart = Chart(id=chart_id)
            chart.metadata = ChartMetadata(chart=chart)
            force_recreate = True

        metadata = chart.metadata

        if force_recreate:
            chart.values = {'x': [], 'y' : []}
            start_date = first_upload.get().upload_time
            metadata.computed_start_date = start_date
            start_closed_interval = True
        else:
            start_date = metadata.computed_end_date
            start_closed_interval = False

        metadata.computed_end_date = end_date

        return (chart, metadata, start_date, end_date, start_closed_interval)


    @staticmethod
    def merge_chart_data(chart_values : dict[str, list],
                         new_data : dict[str, list]) -> dict[str, list]:
        result = copy.deepcopy(chart_values)

        if len(result['x']) > 0 and result['x'][-1] == new_data['x'][0]:
            result['y'][-1] += new_data['y'][0]
            new_data['x'].pop(0)
            new_data['y'].pop(0)

        result['x'] += new_data['x']
        result['y'] += new_data['y']

        return result


    @staticmethod
    def compute_server_count_by_month(force_recreate : bool):
        (chart, metadata,
         start_date, end_date,
         start_closed_interval
        ) = Command.get_computation_object('server-count', force_recreate)

        data = charts.compute_server_count_by_month(start_date, end_date,
                                                    start_closed_interval)

        chart.title = 'Server Count by Month'

        chart.values = Command.merge_chart_data(chart.values, data)

        chart.save()
        metadata.save()



    def handle(self, *args, **options):
        try:
            Command.compute_server_count_by_month(options['recreate'])
        except DatabaseHasNoUploads:
            raise CommandError('No uploads, can not compute charts!')

