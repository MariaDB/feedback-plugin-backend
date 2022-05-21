import datetime
from zoneinfo import ZoneInfo

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, Client
from django.urls import reverse

from .etl import (process_raw_data, compute_upload_facts,
                  compute_all_server_facts, compute_all_server_facts,
                  add_server_system_facts, add_os_srv_fact_if_missing)
# from .etl import *
from .models import RawData, ComputedUploadFact, ComputedServerFact, Data, Server, Upload


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


class ProcessRawData(TestCase):
  def test_process_raw_data(self):
    file_content = b'FEEDBACK_SERVER_UID\thLHc4QZlbY1khIQIFF1T7A6tj04=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'
    file_content_server_2 = b'FEEDBACK_SERVER_UID\tAABBCCDD=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'
    file_content_mysql_test = b'FEEDBACK_SERVER_UID\tFFEEDDCC=\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\tmysql-test\n'
    file_content_empty_uid = b'FEEDBACK_SERVER_UID\t\x00\nFEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'
    file_content_no_uid = b'FEEDBACK_WHEN\tstartup\nFEEDBACK_USER_INFO\t\n'


    time = datetime.datetime(year=2022, month=1, day=2, hour=11, minute=10,
                             tzinfo=ZoneInfo('Europe/Berlin'))
    time2 = datetime.datetime(year=2022, month=1, day=5, hour=12, minute=15,
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

    self.assertEqual(Server.objects.all().count(), 2)
    self.assertEqual(Upload.objects.all().count(), 3)
    self.assertEqual(Upload.objects.filter(server_id=1).count(), 2)
    self.assertEqual(Upload.objects.filter(server_id=2).count(), 1)
    self.assertEqual(ComputedServerFact.objects.all().count(), 8)
    self.assertEqual(Data.objects.all().count(), 9)
    self.assertEqual(RawData.objects.all().count(), 0)

    self.assertEqual(ComputedServerFact.objects.filter(name='uid').count(), 2)
    self.assertEqual(ComputedServerFact.objects.filter(name='first_seen').count(), 2)
    self.assertEqual(ComputedServerFact.objects.filter(name='last_seen').count(), 2)
    self.assertEqual(ComputedServerFact.objects.filter(name='country_code').count(), 2)

    self.assertEqual(ComputedServerFact.objects.
                       filter(name='first_seen', server_id=1)[0].value,
                       '2022-01-02 10:10:00+00:00')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='last_seen', server_id=1)[0].value,
                       '2022-01-05 10:15:00+00:00')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='uid', server_id=1)[0].value,
                       'hLHc4QZlbY1khIQIFF1T7A6tj04=')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='uid', server_id=2)[0].value,
                       'AABBCCDD=')
    self.assertEqual(ComputedServerFact.objects.
                       filter(name='country_code', server_id=2)[0].value,
                       'UA')


class ProcessUploadData(TestCase):
  def test_process_upload_data(self):
    s1 = Server()
    time1 = datetime.datetime.now(datetime.timezone.utc)
    time2 = time1 - datetime.timedelta(seconds=3600)
    u1 = Upload(upload_time=time1, server=s1)
    u2 = Upload(upload_time=time2, server=s1)
    #Data(key='FEEDBACK_SERVER_UID', value='s1', upload=u1)
    #Data(key='FEEDBACK_SERVER_UID', value='s1', upload=u2)
    d1 = Data(key='VERSION', value='10.6.4-MariaDB', upload=u1)
    d2 = Data(key='VERSION', value='10.6.4-MariaDB', upload=u2)

    s1.save()
    u1.save()
    u2.save()
    d1.save()
    d2.save()

    # Check for duplicates in the Computed Upload Facts table
    u3 = Upload(upload_time=time1, server=s1)
    d3 = Data(key='VERSION', value='10.6.4-MariaDB', upload=u3)
    d4 = Data(key='VERSION', value='10.6.4-MariaDB', upload=u3)

    u3.save()
    d3.save()
    d4.save()

    compute_upload_facts(time2, time1)

    # self.assertEqual(ComputedUploadFact.objects.filter(name='version_major', value='10').count(), 2)
    # self.assertEqual(ComputedUploadFact.objects.filter(name='version_minor', value='6').count(), 2)
    # self.assertEqual(ComputedUploadFact.objects.filter(name='version_point', value='4').count(), 2)



    self.assertEqual(ComputedUploadFact.objects.filter(name='version_major', value='10').count(), 3)
    self.assertEqual(ComputedUploadFact.objects.filter(name='version_minor', value='6').count(), 3)
    self.assertEqual(ComputedUploadFact.objects.filter(name='version_point', value='4').count(), 3)


class ProcessServerData(TestCase):
  def test_process_server_system_facts(self):

    time1 = datetime.datetime.now(datetime.timezone.utc)
    time2 = time1 - datetime.timedelta(seconds=3600)

    # Server declaration
    s1 = Server()
    s2 = Server()
    s3 = Server()
    s3 = Server()
    s4 = Server()
    s5 = Server()
    s6 = Server()
    s7 = Server()


    # Upload declaration
    u1 = Upload(upload_time=time1, server=s1)
    u2 = Upload(upload_time=time2, server=s2)
    u3 = Upload(upload_time=time1, server=s2)
    u4 = Upload(upload_time=time2, server=s3)
    u5 = Upload(upload_time=time2, server=s4)
    u6 = Upload(upload_time=time2, server=s5)
    u7 = Upload(upload_time=time2, server=s6)
    u8 = Upload(upload_time=time2, server=s7)


    # Server 1 data creation
    f1_s1 = Data(upload=u1, key='uname_machine', value='x86_64')
    f2_s1 = Data(upload=u1, key='uname_sysname', value='Linux')
    f3_s1 = Data(upload=u1, key='Uname_version',
                 value='#1 SMP Wed Mar 23 09:04:02 UTC 2022')
    f4_s1 = Data(upload=u1, key='Uname_distribution',
                 value='os: NAME=Gentoo')

    # Server 2 data creation
    f1_s2 = Data(upload=u2, key='uname_sysname', value='Linux')
    f2_s2 = Data(upload=u2, key='uname_machine', value='x86_64')
    f3_s2 = Data(upload=u2, key='Uname_version',
                 value='#1 SMP Wed Mar 23 09:04:02 UTC 2022')
    f4_s2 = Data(upload=u2, key='Uname_distribution',
                 value='os: NAME=Gentoo')
    f5_s2 = Data(upload=u3, key='uname_sysname', value='Windows')
    f6_s2 = Data(upload=u3, key='uname_machine', value='x86')
    f7_s2 = Data(upload=u3, key='Uname_version',
                 value='#7 SMP PREEMPT Tue Apr 26 09:03:29 CEST 2022')
    f8_s2 = Data(upload=u3, key='Uname_distribution',
                 value='centos: CentOS release 6.9 (Final)')

    # Server 3 data creation
    f1_s3 = Data(upload=u4, key='VERSION', value='10.6.4-MariaDB')
    # Server 4 data creation
    f1_s4 = Data(upload=u5, key='uname_machine', value='x86_64')
    # Server 5 data creation
    f1_s5 = Data(upload=u6, key='uname_sysname', value='Linux')
    # Server 6 data creation
    f1_s6 = Data(upload=u7, key='Uname_version',
                 value='#1 SMP Wed Mar 23 09:04:02 UTC 2022')
    # Server 7 data creation
    f1_s7 = Data(upload=u8, key='Uname_distribution',
                 value='os: NAME=Gentoo')
    s1.save()
    s2.save()
    s3.save()
    s4.save()
    s5.save()
    s6.save()
    s7.save()

    u1.save()
    u2.save()
    u3.save()
    u4.save()
    u5.save()
    u6.save()
    u7.save()
    u8.save()

    # Server 1 facts save
    f1_s1.save()
    f2_s1.save()
    f3_s1.save()
    f4_s1.save()

    # Server 2 facts save
    f1_s2.save()
    f2_s2.save()
    f3_s2.save()
    f4_s2.save()
    f5_s2.save()
    f6_s2.save()
    f7_s2.save()
    f8_s2.save()

    # Server 3 facts save
    f1_s3.save()
    # Server 4 facts save
    f1_s4.save()
    # Server 5 facts save
    f1_s5.save()
    # Server 6 facts save
    f1_s6.save()
    # Server 7 facts save
    f1_s7.save()

    add_server_system_facts(time2, time1)

    self.assertEqual(Server.objects.all().count(), 7)
    self.assertEqual(Upload.objects.all().count(), 8)
    self.assertEqual(Data.objects.all().count(), 17)
    self.assertEqual(ComputedServerFact.objects.all().count(), 12)

    # Server 1 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s1,
                        name='architecture').value,
                      'x86_64')
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s1,
                        name='os_name').value,
                      'Linux')
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s1,
                        name='os_version').value,
                      'unknown')
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s1,
                        name='distribution').value,
                      'os: NAME=Gentoo')

    # Server 2 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s2,
                        name='architecture').value,
                      'x86')
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s2,
                        name='os_name').value,
                      'Windows')
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s2,
                        name='os_version').value,
                      'unknown')
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s2,
                        name='distribution').value,
                      'centos: CentOS release 6.9 (Final)')

    # Server 3 expected results
    with self.assertRaises(ComputedServerFact.DoesNotExist):
      ComputedServerFact.objects.get(server=s3,name='VERSION').value


    # Server 4 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s4,
                        name='architecture').value,
                      'x86_64')
    # Server 5 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s5,
                        name='os_name').value,
                      'Linux')
    # Server 6 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s6,
                        name='os_version').value,
                      'unknown')
    # Server 7 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s7,
                        name='distribution').value,
                      'os: NAME=Gentoo')


  def test_process_server_os_facts(self):

    time1 = datetime.datetime.now(datetime.timezone.utc)
    time2 = time1 - datetime.timedelta(seconds=3600)

    # Server declaration
    s1 = Server()
    s2 = Server()

    # Upload declaration
    u1 = Upload(upload_time=time1, server=s1)
    u2 = Upload(upload_time=time2, server=s2)

    f1 = ComputedServerFact(server=s1, name='os_name', value='Windows')

    s1.save()
    s2.save()
    u1.save()
    u2.save()
    f1.save()

    add_os_srv_fact_if_missing(time2, time1)

    self.assertEqual(Server.objects.all().count(), 2)
    self.assertEqual(Upload.objects.all().count(), 2)
    self.assertEqual(ComputedServerFact.objects.all().count(), 2)

    # Server 1 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s1,
                        name='os_name').value,
                     'Windows')

    # Server 1 expected results
    self.assertEqual(ComputedServerFact.objects.get(
                        server=s2,
                        name='os_name').value,
                     'Linux')
