import random
from datetime import datetime, timedelta, timezone

from django.test import TestCase

from feedback_plugin.data_processing.charts import compute_server_count_by_month
from feedback_plugin.models import Server, Upload

class ComputeServerCount(TestCase):
  def test_basic(self):

    TOTAL_SERVERS = 9
    SERVERS_PER_MONTH = [4, 3, 2, 1]
    UPLOADS_PER_MONTH_PER_SERVER = 5

    times = []
    base_time = datetime(year=2022, month=2, day=15, tzinfo=timezone.utc)
    for i in range(len(SERVERS_PER_MONTH)):
      times.append(base_time - timedelta(days=31 * i))

    servers = []
    for i in range(TOTAL_SERVERS):
      s = Server()
      servers.append(s)
      s.save()

    uploads = []
    for i in range(len(SERVERS_PER_MONTH)):
      picked_servers = random.sample(servers, k=SERVERS_PER_MONTH[i])
      for server in picked_servers:
        for j in range(UPLOADS_PER_MONTH_PER_SERVER):
          u = Upload(
                upload_time=times[i] + timedelta(days=random.randint(1, 5)),
                server=server)
          u.save()

    # Test with a time interval over the random value introduced above.
    self.assertEqual(compute_server_count_by_month(times[-1],
                                                   times[0] + timedelta(days=6),
                                                   True),
                     {
                         'count': {
                             'x': ['2021-11', '2021-12', '2022-01', '2022-02'],
                             'y': [1, 2, 3, 4],
                         },
                     })

    self.assertEqual(compute_server_count_by_month(times[-2],
                                                   times[0] + timedelta(days=6),
                                                   True),
                     {
                         'count': {
                             'x': ['2021-12', '2022-01', '2022-02'],
                             'y': [2, 3, 4],
                         },
                     })
    self.assertEqual(compute_server_count_by_month(times[-2],
                                                   times[1] + timedelta(days=6),
                                                   True),
                     {
                         'count': {
                             'x': ['2021-12', '2022-01'],
                             'y': [2, 3],
                         },
                     })

    s_edge = Server()
    s_edge.save()

    first_upload_time = datetime(year=2022, month=5, day=2, hour=10,
                                 minute=10, tzinfo=timezone.utc)
    second_upload_time = datetime(year=2022, month=6, day=2, hour=10,
                                 minute=10, tzinfo=timezone.utc)
    Upload(upload_time=first_upload_time, server=s_edge).save()
    Upload(upload_time=second_upload_time, server=s_edge).save()


    self.assertEqual(compute_server_count_by_month(first_upload_time,
                                                   second_upload_time, False),
                     {
                         'count': {
                             'x': ['2022-06'],
                             'y': [1],
                         },
                     })

    self.assertEqual(compute_server_count_by_month(first_upload_time,
                                                   second_upload_time, True),
                     {
                         'count': {
                             'x': ['2022-05', '2022-06'],
                             'y': [1, 1],
                         },
                     })
