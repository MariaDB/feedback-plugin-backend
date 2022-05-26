from datetime import datetime, timezone
import os
from zoneinfo import ZoneInfo

from django.test import TestCase

from feedback_plugin.etl import process_raw_data
from feedback_plugin.models import (RawData, Server, Upload, Data,
                                    ComputedServerFact)
from feedback_plugin.tests.utils import load_test_data, create_test_database

class ProcessRawData(TestCase):
  def test_process_raw_data(self):
    file_content = b'FEEDBACK_SERVER_UID\thLHc4QZlbY1khIQIFF1T7A6tj04=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'
    file_content_server_2 = b'FEEDBACK_SERVER_UID\tAABBCCDD=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'
    file_content_mysql_test = b'FEEDBACK_SERVER_UID\tFFEEDDCC=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\tmysql-test\n'
    file_content_empty_uid = b'FEEDBACK_SERVER_UID\t\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'
    file_content_no_uid = b'FEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'


    time = datetime(year=2022, month=1, day=2, hour=11, minute=10,
                    tzinfo=ZoneInfo('Europe/Berlin'))
    time2 = datetime(year=2022, month=1, day=5, hour=12, minute=15,
                     tzinfo=ZoneInfo('Europe/Bucharest'))

    s1_d1 = RawData(country='US', data=file_content, upload_time=time)
    s1_d2 = RawData(country='US', data=file_content, upload_time=time2)
    s2_d1 = RawData(country='UA', data=file_content_server_2, upload_time=time)
    # S3 should be discarded after processing.
    s3_d1 = RawData(country='US', data=file_content_mysql_test, upload_time=time)
    s4_d1 = RawData(country='US', data=file_content_empty_uid, upload_time=time)
    s5_d1 = RawData(country='US', data=file_content_no_uid, upload_time=time)
    s1_d1.save()
    s1_d2.save()
    s2_d1.save()
    s3_d1.save()
    s4_d1.save()
    s5_d1.save()

    process_raw_data()

    servers = Server.objects.all()
    self.assertEqual(servers.count(), 2)
    s1 = servers[0]
    s2 = servers[1]
    self.assertEqual(Upload.objects.all().count(), 3)
    self.assertEqual(Upload.objects.filter(server_id=s1.id).count(), 2)
    self.assertEqual(Upload.objects.filter(server_id=s2.id).count(), 1)
    self.assertEqual(ComputedServerFact.objects.all().count(), 8)
    self.assertEqual(Data.objects.all().count(), 9)
    self.assertEqual(RawData.objects.all().count(), 0)

    self.assertEqual(ComputedServerFact.objects.filter(name='uid').count(), 2)
    self.assertEqual(ComputedServerFact.objects.filter(name='first_seen').count(), 2)
    self.assertEqual(ComputedServerFact.objects.filter(name='last_seen').count(), 2)
    self.assertEqual(ComputedServerFact.objects.filter(name='country_code').count(), 2)

    self.assertEqual(ComputedServerFact.objects.
                       filter(name='first_seen', server_id=s1.id)[0].value,
                       '2022-01-02 10:10:00+00:00')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='last_seen', server_id=s1.id)[0].value,
                       '2022-01-05 10:15:00+00:00')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='uid', server_id=s1.id)[0].value,
                       'hLHc4QZlbY1khIQIFF1T7A6tj04=')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='uid', server_id=s2.id)[0].value,
                       'AABBCCDD=')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='country_code', server_id=s2.id)[0].value,
                       'UA')

  def test_load_fixtures(self):
    test_data = load_test_data(os.path.join(
                                 os.path.dirname(os.path.realpath(__file__)),
                                 'test_data/'))

    create_test_database(test_data)

    self.assertEqual(Upload.objects.all().count(), 8)
    self.assertEqual(Server.objects.all().count(), 5)
    self.assertEqual(ComputedServerFact.objects.all().count(), 20)
