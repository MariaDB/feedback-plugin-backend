import datetime

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, Client
from django.urls import reverse

from feedback_plugin.models import RawData, Config

class FilePostTest(TestCase):
    def test_file_upload(self):
        '''
        Test that uploading a file results in storing the contents as-is
        into the database.
        '''
        c = Client()

        response = c.post(reverse('file_post'), data={'data': ''},
                        HTTP_X_REAL_IP='127.0.0.1')
        self.assertEqual(response.status_code, 400)

        file_content = b'FEEDBACK_SERVER_UID\thLHc4QZlbY1khIQIFF1T7A6tj04=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'

        file = SimpleUploadedFile('report.csv', file_content,
                                  content_type='application/octet-stream')

        response = c.post(reverse('file_post'), data={'data': file},
                          HTTP_X_REAL_IP='127.0.0.1')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b'<h1>ok</h1>')

        raw_data_objects = RawData.objects.all()

        self.assertEqual(raw_data_objects.count(), 1)
        self.assertEqual(raw_data_objects[0].data, file_content)
        self.assertEqual(raw_data_objects[0].country.code, 'ZZ')

        # Second post also gets saved to the database. Use OSUOSL ip to test.
        file = SimpleUploadedFile('report.csv', file_content,
                                  content_type='application/octet-stream')
        response = c.post(reverse('file_post'), data={'data': file},
                          REMOTE_ADDRESS='140.211.166.134')

        raw_data_objects = RawData.objects.all().order_by('id')

        self.assertEqual(raw_data_objects.count(), 2)
        self.assertEqual(raw_data_objects[1].data, file_content)
        self.assertEqual(raw_data_objects[1].country.code, 'US')

        response = c.get(reverse('file_post'))
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.headers['Allow'], 'POST')

    def test_file_upload_with_api_key(self):
        c = Client()

        file_content = b'FEEDBACK_SERVER_UID\thLHc4QZlbY1khIQIFF1T7A6tj04=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'
        file = SimpleUploadedFile('report.csv', file_content,
                                  content_type='application/octet-stream')

        # Test error cases.
        response = c.post(reverse('file_post_protected'), data={'data': file})
        self.assertEqual(response.content, b'No X_API_KEY configured for Server')
        self.assertEqual(response.status_code, 403)

        Config(key='X_API_KEY', value='secr3t').save()

        file = SimpleUploadedFile('report.csv', file_content,
                                  content_type='application/octet-stream')

        # No X_API_KEY provided for header.
        response = c.post(reverse('file_post_protected'), data={'data': file})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(RawData.objects.all().count(), 0)

        file = SimpleUploadedFile('report.csv', file_content,
                                  content_type='application/octet-stream')

        # Wrong X_API_KEY provided.
        response = c.post(reverse('file_post_protected'), data={'data': file},
                          X_API_KEY='s3cr3t_wrong')
        self.assertEqual(response.status_code, 403)
        self.assertEqual(RawData.objects.all().count(), 0)

        # Correct report.
        file = SimpleUploadedFile('report.csv', file_content,
                                  content_type='application/octet-stream')
        response = c.post(reverse('file_post_protected'), data={'data': file},
                          X_API_KEY='secr3t',
                          X_REPORT_FROM_IP='140.211.166.134',
                          X_REPORT_DATE='2019-01-02 12:03:33.000004')

        raw_data_objects = RawData.objects.all().order_by('id')

        self.assertEqual(raw_data_objects.count(), 1)
        self.assertEqual(raw_data_objects[0].data, file_content)
        self.assertEqual(raw_data_objects[0].country.code, 'US')
        self.assertEqual(raw_data_objects[0].upload_time,
                         datetime.datetime(year=2019, month=1, day=2,
                                           hour=12, minute=3, second=33, microsecond=4,
                                           tzinfo=datetime.timezone.utc))
