import copy
import sys

from django.core.management.base import BaseCommand, CommandError

from feedback_plugin.models import (Chart, ChartMetadata, Upload)
from feedback_plugin.data_processing import charts

from sql_utils.utils import print_sql

class DatabaseHasNoUploads(Exception):
    pass

CHARTS_MAP = {
        'server-count' : {
            'callback' : charts.compute_server_count_by_month,
            'title' : 'Server Count by Month',
        },
        'version-breakdown-by-month' : {
            'callback' : charts.compute_version_breakdown_by_month,
            'title' : 'Server Version Breakdown by Month',
        },
}

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--recreate', action='store_true')
        parser.add_argument('--chart', default='all')

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
            chart.values = {}
            start_date = first_upload.get().upload_time
            metadata.computed_start_date = start_date
            start_closed_interval = True
        else:
            start_date = metadata.computed_end_date
            start_closed_interval = False

        metadata.computed_end_date = end_date

        return (chart, metadata, start_date, end_date, start_closed_interval)


    @staticmethod
    def merge_multi_series_chart_data(chart_values : dict[str, dict[str, list]],
                                      new_data : dict[str, dict[str, list]]
    ) -> dict[str, dict[str, list]]:
        result = {}
        for series in chart_values:
            new_data_series = {}
            if series in new_data:
                new_data_series = new_data[series]

            result[series] = Command.merge_chart_data(chart_values[series],
                                                      new_data_series)
        for series in new_data:
            if series not in result:
                result[series] = copy.deepcopy(new_data[series])
        return result

    @staticmethod
    def merge_chart_data(chart_values : dict[str, list],
                         new_data : dict[str, list]) -> dict[str, list]:
        result = copy.deepcopy(chart_values)

        if 'x' not in new_data:
            return result

        if len(result['x']) > 0 and result['x'][-1] == new_data['x'][0]:
            result['y'][-1] += new_data['y'][0]
            new_data['x'].pop(0)
            new_data['y'].pop(0)

        result['x'] += new_data['x']
        result['y'] += new_data['y']

        return result

    @staticmethod
    def compute_chart(chart_id : str, title : str, callback, force_recreate : bool):
        (chart, metadata,
         start_date, end_date,
         start_closed_interval
        ) = Command.get_computation_object(chart_id, force_recreate)

        data = callback(start_date, end_date, start_closed_interval)

        chart.title = title

        chart.values = Command.merge_multi_series_chart_data(chart.values, data)

        chart.save()
        metadata.save()

    def handle(self, *args, **options):
        try:
            all = options['chart'] == 'all'
            if not all:
                if options['chart'] not in CHARTS_MAP:
                    raise CommandError('Invalid chart id')

                Command.compute_chart(options['chart'],
                                      CHARTS_MAP[options['chart']]['title'],
                                      CHARTS_MAP[options['chart']]['callback'],
                                      options['recreate'])
            else:
                for chart_id in CHARTS_MAP:
                    Command.compute_chart(chart_id,
                                          CHARTS_MAP[chart_id]['title'],
                                          CHARTS_MAP[chart_id]['callback'],
                                          options['recreate'])
        except DatabaseHasNoUploads:
            raise CommandError('No uploads, can not compute charts!')
