from datetime import datetime, timezone, timedelta

from django.test import TestCase

from feedback_plugin.etl import compute_upload_facts
from feedback_plugin.models import Server, Upload, Data, ComputedUploadFact

class TestComputeVersion(TestCase):
  def test(self):
    s1 = Server()
    time1 = datetime.now(timezone.utc)
    time2 = time1 - timedelta(seconds=3600)
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

