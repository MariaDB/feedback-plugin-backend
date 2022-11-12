from datetime import datetime, timezone, timedelta

from django.test import TestCase

from feedback_plugin.data_processing.extractors import ServerVersionExtractor
from feedback_plugin.data_processing.etl import extract_upload_facts
from feedback_plugin.models import Server, Upload, Data, ComputedUploadFact

class TestComputeVersion(TestCase):
  def test(self):
    s1 = Server()
    time1 = datetime.now(timezone.utc)
    time2 = time1 - timedelta(seconds=3600)
    u1 = Upload(upload_time=time1, server=s1)
    u2 = Upload(upload_time=time2, server=s1)
    d1 = Data(key='VERSION', value='10.6.1-MariaDB', upload=u1)
    d2 = Data(key='VERSION', value='10.6.2-MariaDB', upload=u2)

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

    extract_upload_facts(time2, time1, [ServerVersionExtractor()])

    self.assertEqual(ComputedUploadFact.objects.filter(key='server_version_major', value='10').count(), 3)
    self.assertEqual(ComputedUploadFact.objects.filter(key='server_version_minor', value='6').count(), 3)
    self.assertEqual(ComputedUploadFact.objects.filter(key='server_version_point', value='1').count(), 1)
    self.assertEqual(ComputedUploadFact.objects.filter(key='server_version_point', value='2').count(), 1)
    self.assertEqual(ComputedUploadFact.objects.filter(key='server_version_point', value='4').count(), 1)
