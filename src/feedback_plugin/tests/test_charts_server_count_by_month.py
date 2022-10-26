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
                upload_time=times[i] + timedelta(days=random.randint(-3, 3)),
                server=server)
          u.save()

    self.assertEqual(compute_server_count_by_month(times[-1], times[0]),
                     {
                       'x': ['2021-11', '2021-12', '2022-01', '2022-02'],
                       'y': [1, 2, 3, 4]
                     })



