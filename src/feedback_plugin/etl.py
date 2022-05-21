import csv
import re
from datetime import datetime, timezone
from io import StringIO

from .models import (ComputedServerFact, ComputedUploadFact, Data,
                     RawData, Server, Upload)
from django.db.models import Q

def process_raw_data():
  for raw_upload in RawData.objects.order_by('upload_time').iterator():

    country_code = raw_upload.country
    raw_upload_time = raw_upload.upload_time
    raw_data = StringIO(raw_upload.data.decode('utf-8').replace('\x00', ''))

    reader = csv.reader(raw_data, delimiter='\t')

    values = []
    data = {}
    for row in reader:
      # We only expect KV pairs.
      if len(row) != 2:
        continue

      data[row[0]] = row[1]
      values.append(Data(key=row[0], value=row[1]))

    if ('FEEDBACK_SERVER_UID' not in data
        or len(data['FEEDBACK_SERVER_UID']) == 0):
      continue

    uid = data['FEEDBACK_SERVER_UID']

    if ('FEEDBACK_USER_INFO' in data
        and data['FEEDBACK_USER_INFO'] == 'mysql-test'):
      continue


    try:
      srv_uid_fact = ComputedServerFact.objects.get(name='uid', value=uid)
      server = Server.objects.get(id=srv_uid_fact.server.id)
    except ComputedServerFact.DoesNotExist:

      server = Server()
      srv_uid_fact = ComputedServerFact(name='uid',
                                        value=uid,
                                        server=server)
      server.save()
      srv_uid_fact.save()

    except Server.DoesNotExist:
      # We have a fact, but not a server attached to it. This should never
      # happen.
      assert(0)

    srv_facts = []
    def get_fact_by_name(name, server, value):
      try:
        srv_fact = ComputedServerFact.objects.get(name=name,
                                                  server__id=server.id)
        srv_fact.value = value
        return (False, srv_fact)
      except ComputedServerFact.DoesNotExist:
        return (True, ComputedServerFact(name=name, value=value, server=server))

    srv_facts.append(get_fact_by_name('country_code', server, country_code))
    srv_facts.append(get_fact_by_name('last_seen', server, raw_upload_time))
    srv_facts.append(get_fact_by_name('first_seen', server, raw_upload_time))
    # If first_seen already existed, we don't override it.
    if (srv_facts[-1][0] == False):
      srv_facts.pop()

    srv_facts_create = map(lambda x: x[1], filter(lambda x: x[0], srv_facts))
    srv_facts_update = map(lambda x: x[1], filter(lambda x: not x[0], srv_facts))

    # Create Server computed facts for this RawData.
    ComputedServerFact.objects.bulk_create(srv_facts_create)
    ComputedServerFact.objects.bulk_update(srv_facts_update, ['value'])

    # Create Upload and all Data entries for this RawData.
    upload = Upload(upload_time=raw_upload_time, server=server)

    for i in range(len(values)):
      values[i].upload = upload
    upload.save()
    Data.objects.bulk_create(values)

  RawData.objects.all().delete()


def compute_upload_facts(start_date, end_date):

  data_to_process = Data.objects.filter(upload__upload_time__gte=start_date,
                      upload__upload_time__lte=end_date,
                      key='VERSION')

  pattern = re.compile('(?P<major>\d+).(?P<minor>\d+).(?P<point>\d+)')
  version_facts = []
  version_dict = {}

  for data in data_to_process:
    matches = pattern.match(data.value)
    if (data.upload_id not in version_dict):
      version_dict[data.upload_id] = {}

    version_dict[data.upload_id]['major'] = ComputedUploadFact(
                         name='version_major',
                         value=matches.group('major'),
                         upload=data.upload)
    version_dict[data.upload_id]['minor'] = ComputedUploadFact(
                         name='version_minor',
                         value=matches.group('minor'),
                         upload=data.upload)
    version_dict[data.upload_id]['point'] = ComputedUploadFact(
                         name='version_point',
                         value=matches.group('point'),
                         upload=data.upload)

  for upload_id, version in version_dict.items():
    version_facts.append(version['major'])
    version_facts.append(version['minor'])
    version_facts.append(version['point'])

  ComputedUploadFact.objects.bulk_create(version_facts)



def add_server_system_facts(start_date, end_date):
  # Extract server data about Architecture and OS
  data_to_process = Data.objects.filter(
                      Q(upload__upload_time__gte=start_date) &
                      Q(upload__upload_time__lte=end_date) &
                      (Q(key='uname_machine')
                       | Q(key='uname_sysname')
                       | Q(key='Uname_version')
                       | Q(key='Uname_distribution'))).select_related(
                      'upload__server')

  system_dict = {}
  system_fact = []

  for data in data_to_process:
    # In case there are multiple entries for the same server, use the
    # latest one only
    server_id = data.upload.server.id
    if (server_id not in system_dict):
      system_dict[server_id] = {}

    system_dict[server_id][data.key] = ComputedServerFact(
                                         server=data.upload.server,
                                         name=data.key,
                                         value=data.value)

  for server_id, fact_dict in system_dict.items():
    if 'uname_machine' in fact_dict:
      def clean_arch_entries(arch):

        if arch.startswith('HP'):
          return 'HP'

        return arch

      # Change the fact name to something easy to understand
      fact_dict['uname_machine'].name = 'architecture'
      # Clean up architecture names to something readable
      arch = clean_arch_entries(fact_dict['uname_machine'].value)
      fact_dict['uname_machine'].value = arch
      system_fact.append(fact_dict['uname_machine'])

    if 'uname_sysname' in fact_dict:
      fact_dict['uname_sysname'].name = 'os_name'
      system_fact.append(fact_dict['uname_sysname'])

    if 'Uname_version' in fact_dict:
      def clean_os_version(os_ver):
        if os_ver.startswith('Windows'):
          return os_ver

        # For cases like '#1 SMP Wed Mar 23 09:04:02 UTC 2022'
        if 'SMP' in os_ver:
          return 'unknown' #TODO: see if there is a better solution.
        return 'unknown'

      # Change the fact name to something easy to understand
      fact_dict['Uname_version'].name = 'os_version'
      # Clean up architecture names to something readable
      os_ver = clean_os_version(fact_dict['Uname_version'].value)
      fact_dict['Uname_version'].value = os_ver
      system_fact.append(fact_dict['Uname_version'])

    if 'Uname_distribution' in fact_dict:
      # def clean_distribution(dist):

      fact_dict['Uname_distribution'].name = 'distribution'
      system_fact.append(fact_dict['Uname_distribution'])

  ComputedServerFact.objects.bulk_create(system_fact)


def add_os_srv_fact_if_missing(start_date, end_date):

  # Get the server list where the OS name exists
  servers_with_os = ComputedServerFact.objects.filter(
                      name='os_name').values_list(
                      'server_id',
                      flat=True)
  #print('srv_with_os', servers_with_os)
  servers_to_process = Server.objects.exclude(
                          id__in=servers_with_os)
  #print('srv_without_os', servers_to_process)

  update_list = []
  for srv in servers_to_process:

    fact = ComputedServerFact(server=srv,
                      name='os_name',
                      value='Linux')
    update_list.append(fact)

  ComputedServerFact.objects.bulk_create(update_list)



def compute_all_server_facts(start_date, end_date):


  compute_server_system_facts(start_date, end_date)
  add_os_srv_fact_if_missing(start_date, end_date)

    # if (data.upload_id not in version_dict):
    #   version_dict[data.upload_id] = {}

  # server_to_process = Server.objects.filter(
  #                       upload__upload_time__gte=start_date,
  #                       upload__upload_time__lte=end_date
  #                       ).prefetch_related(
  #                       'computedserverfact_set')
  # print('srv to process', server_to_process)
  # for server in server_to_process:
    # print(server)
    # for fact in server.computedserverfact_set:
      # if fact.name == 'first_seen':
        # continue

      # print(fact)



  # data_to_process = Upload.objects.filter(
  #                       upload_time__gte=start_date,
  #                       upload_time__lte=end_date)




  # data_to_process = ComputedServerFact.objects.filter(
  #                     server__upload__upload_time__gte=start_date,
  #                     server__upload__upload_time__lte=end_date)

  # data_to_process = Data.objects.filter(
  #                     upload__upload_time__gte=start_date,
  #                     upload__upload_time__lte=end_date,
  #                     upload__server__computedserverfact__name='UPTIME').select_related(
  #                     # 'upload__server__name'
  #                     # 'upload__server__value'
  #                     'upload',
  #                     'upload__server'
  #                     )


