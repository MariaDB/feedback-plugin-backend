from datetime import datetime, timedelta, timezone

from django.test import TestCase, Client

from feedback_plugin.data_processing.etl import (extract_server_facts,
                                                 add_os_srv_fact_if_missing)
from feedback_plugin.tests.utils import create_test_database
from feedback_plugin.data_processing.extractors import ArchitectureExtractor
from feedback_plugin.models import Data, Upload, Server, ComputedServerFact

class ComputeOS(TestCase):
  def test_linux(self):

    time1 = datetime.now(timezone.utc)
    time2 = time1 - timedelta(seconds=3600)

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


    def check_expected_results():
        self.assertEqual(Server.objects.all().count(), 7)
        self.assertEqual(Upload.objects.all().count(), 8)
        self.assertEqual(Data.objects.all().count(), 17)
        self.assertEqual(ComputedServerFact.objects.all().count(), 12)

        # Server 1 expected results
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s1,
                            key='hardware_architecture').value,
                          'x86_64')
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s1,
                            key='operating_system').value,
                          'Linux')
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s1,
                            key='operating_system_version').value,
                          'unknown')
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s1,
                            key='distribution').value,
                          'Gentoo')

        # Server 2 expected results
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s2,
                            key='hardware_architecture').value,
                          'x86')
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s2,
                            key='operating_system').value,
                          'Windows')
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s2,
                            key='operating_system_version').value,
                          'unknown')
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s2,
                            key='distribution').value,
                          'CentOS')

        # Server 3 expected results
        with self.assertRaises(ComputedServerFact.DoesNotExist):
          ComputedServerFact.objects.get(server=s3,key='VERSION').value


        # Server 4 expected results
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s4,
                            key='hardware_architecture').value,
                          'x86_64')
        # Server 5 expected results
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s5,
                            key='operating_system').value,
                          'Linux')
        # Server 6 expected results
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s6,
                            key='operating_system_version').value,
                          'unknown')
        # Server 7 expected results
        self.assertEqual(ComputedServerFact.objects.get(
                            server=s7,
                            key='distribution').value,
                          'Gentoo')

    # Test first insert
    extract_server_facts(time2, time1, [ArchitectureExtractor()])
    check_expected_results()
    # Test update
    extract_server_facts(time2, time1, [ArchitectureExtractor()])
    check_expected_results()


  def test_on_all(self):
    create_test_database()

    begin = datetime(year=2022, month=1, day=1, tzinfo=timezone.utc)
    end = datetime(year=2022, month=3, day=1, tzinfo=timezone.utc)

    arch_extractor = ArchitectureExtractor()
    extract_server_facts(begin, end, [arch_extractor])


    servers = Server.objects.all()

    extracted_facts = ComputedServerFact.objects.filter(
        key__in= ['operating_system', 'hardware_architecture',
                  'distribution', 'operating_system_version'])

    #print(extracted_facts)
    # One fact for each server.
    self.assertEqual(servers.count(), 5)
    self.assertEqual(extracted_facts.filter(key='operating_system',
                                            value='Windows').count(), 4)
    self.assertEqual(extracted_facts.filter(key='operating_system_version',
                                            value__icontains='windows').count(), 4)
    self.assertEqual(extracted_facts.filter(key='operating_system',
                                            value='Linux').count(),1)
    self.assertEqual(extracted_facts.filter(key='distribution',
                                            value='CentOS').count(), 1)
    self.assertEqual(extracted_facts.filter(key='operating_system_version',
                                            value='8.2.2004 (core)').count(), 1)



