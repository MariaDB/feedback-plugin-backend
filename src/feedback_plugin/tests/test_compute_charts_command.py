import copy
from datetime import timedelta
from io import StringIO

from django.core.management import call_command
from django.core.management.base import CommandError

from django.test import TestCase

from feedback_plugin.tests.utils import create_test_database
from feedback_plugin.models import Server, Upload, Chart, ChartMetadata

class ComputeChartsCommand(TestCase):
    @staticmethod
    def call(*args, **kwargs):
        out = StringIO()
        err = StringIO()

        value = call_command(
            'compute_charts',
            *args,
            stdout=out,
            stderr=err,
            **kwargs
        )

        return (value, out, err)

    def test_call_command_no_parameters(self):
        self.assertRaisesMessage(CommandError,
                                 'No uploads, can not compute charts!',
                                 ComputeChartsCommand.call)
        create_test_database()

        ComputeChartsCommand.call('--recreate')

        first_upload = Upload.objects.all().order_by('upload_time')[:1][0]
        last_upload = Upload.objects.all().order_by('-upload_time')[:1][0]

        self.assertEqual(Chart.objects.all().count(), 2)
        self.assertEqual(ChartMetadata.objects.all().count(), 2)

        chart = Chart.objects.get(id='server-count')
        self.assertEqual(chart.title, 'Server Count by Month')
        self.assertEqual(chart.values,
                         {
                             'count': {
                                 'x': ['2022-01', '2022-02', '2022-03'],
                                 'y': [3, 4, 1],
                             }
                         })
        self.assertEqual(chart.metadata.computed_start_date,
                         first_upload.upload_time)
        self.assertEqual(chart.metadata.computed_end_date,
                         last_upload.upload_time)

        new_last_upload = copy.deepcopy(last_upload)
        new_last_upload.upload_time = last_upload.upload_time + timedelta(days=35)
        new_last_upload.id = None
        new_last_upload.save()

        new_first_upload = copy.deepcopy(first_upload)
        new_first_upload.upload_time = first_upload.upload_time - timedelta(days=20)
        new_first_upload.id = None
        new_first_upload.save()

        # Without recreate parameter, the chart only looks at follow-up data.
        ComputeChartsCommand.call()

        chart.refresh_from_db()
        self.assertEqual(chart.title, 'Server Count by Month')
        self.assertEqual(chart.values,
                         {
                             'count': {
                                 'x': ['2022-01', '2022-02', '2022-03', '2022-04'],
                                 'y': [3, 4, 1, 1],
                             }
                         })
        self.assertEqual(chart.metadata.computed_start_date,
                         first_upload.upload_time)
        self.assertEqual(chart.metadata.computed_end_date,
                         new_last_upload.upload_time)

        # With recreate parameter, the chart is recreated.
        ComputeChartsCommand.call('--recreate')

        chart.refresh_from_db()
        self.assertEqual(chart.title, 'Server Count by Month')
        self.assertEqual(chart.values,
                         {
                             'count': {
                                 'x': ['2021-12', '2022-01', '2022-02', '2022-03', '2022-04'],
                                 'y': [1, 3, 4, 1, 1],
                             }
                         })
        self.assertEqual(chart.metadata.computed_start_date,
                         new_first_upload.upload_time)
        self.assertEqual(chart.metadata.computed_end_date,
                         new_last_upload.upload_time)

    def test_compute_version_breakdown_by_month(self):
        create_test_database()

        ComputeChartsCommand.call('--recreate', '--chart=version-breakdown-by-month')

        self.assertEqual(Chart.objects.all().count(), 1)

        chart = Chart.objects.get(id='version-breakdown-by-month')

        self.assertEqual(chart.title, 'Server Version Breakdown by Month')
        self.assertEqual(chart.values,
                         {
                             '10.1': {
                                 'x': ['2022-2'],
                                 'y': [1]
                             },
                             '10.3': {
                                 'x': ['2022-1', '2022-2', '2022-3'],
                                 'y': [1, 1, 1]
                             },
                             '10.4': {
                                 'x': ['2022-1', '2022-2'],
                                 'y': [2, 2]
                             },
                         })
