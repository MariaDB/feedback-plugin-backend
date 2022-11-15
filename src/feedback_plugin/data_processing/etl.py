import csv
import re
from collections import defaultdict
from datetime import datetime, timezone
from io import StringIO

from feedback_plugin.models import (ComputedServerFact, ComputedUploadFact,
                                    Data, RawData, Server, Upload)
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

    if ('FEEDBACK_USER_INFO' in data
        and data['FEEDBACK_USER_INFO'] == 'mysql-test'):
      continue

    uid = data['FEEDBACK_SERVER_UID']
    try:
      srv_uid_fact = ComputedServerFact.objects.get(key='uid', value=uid)
      server = Server.objects.get(id=srv_uid_fact.server.id)
    except ComputedServerFact.DoesNotExist:
      server = Server()
      srv_uid_fact = ComputedServerFact(key='uid',
                                        value=uid,
                                        server=server)
      server.save()
      srv_uid_fact.save()

    except Server.DoesNotExist:
      # We have a fact, but not a server attached to it. This should never
      # happen.
      assert(0)

    srv_facts = []
    def get_fact_by_key(key, server, value):
      try:
        srv_fact = ComputedServerFact.objects.get(key=key,
                                                  server__id=server.id)
        srv_fact.value = value
        return (False, srv_fact)
      except ComputedServerFact.DoesNotExist:
        return (True, ComputedServerFact(key=key, value=value, server=server))

    srv_facts.append(get_fact_by_key('country_code', server, country_code))
    srv_facts.append(get_fact_by_key('last_seen', server, raw_upload_time))
    srv_facts.append(get_fact_by_key('first_seen', server, raw_upload_time))
    # If first_seen already existed, we don't override it.
    if (srv_facts[-1][0] == False):
      srv_facts.pop()

    srv_facts_create = map(lambda x: x[1], filter(lambda x: x[0], srv_facts))
    srv_facts_update = map(lambda x: x[1], filter(lambda x: not x[0], srv_facts))

    # Create Server computed facts for this RawData.
    ComputedServerFact.objects.bulk_create(srv_facts_create,
                                           batch_size=1000)
    ComputedServerFact.objects.bulk_update(srv_facts_update, ['value'],
                                           batch_size=1000)

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
                         key='version_major',
                         value=matches.group('major'),
                         upload=data.upload)
    version_dict[data.upload_id]['minor'] = ComputedUploadFact(
                         key='version_minor',
                         value=matches.group('minor'),
                         upload=data.upload)
    version_dict[data.upload_id]['point'] = ComputedUploadFact(
                         key='version_point',
                         value=matches.group('point'),
                         upload=data.upload)

  for upload_id, version in version_dict.items():
    version_facts.append(version['major'])
    version_facts.append(version['minor'])
    version_facts.append(version['point'])

  ComputedUploadFact.objects.bulk_create(version_facts, batch_size=1000)


def get_upload_data_for_data_extractors(start_date, end_date,
                                        data_extractors):
  keys = set()
  for extractor in data_extractors:
    keys |= extractor.get_required_keys()
  key_filter = Q()
  for key in keys:
    key_filter |= Q(key__iexact=key)

  # Extract server data about Architecture and OS
  data_to_process = (Data.objects
      .filter(Q(upload__upload_time__gte=start_date) &
              Q(upload__upload_time__lte=end_date) &
              key_filter)
      .select_related('upload__server')
  )

  servers = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
  for data in data_to_process:
    # In case there are multiple entries for the same server, use the
    # latest one only
    server_id = data.upload.server.id
    upload_id = data.upload.id
    # Appending to a list allows for multiple values for the same key.
    servers[server_id][upload_id][data.key.lower()].append(data.value)

  return servers



def extract_server_facts(start_date, end_date, data_extractors):
  servers = get_upload_data_for_data_extractors(start_date, end_date,
                                                data_extractors)
  facts = defaultdict(dict)
  for extractor in data_extractors:
    new_facts = extractor.extract_facts(servers)
    for s_id in new_facts:
      facts[s_id].update(new_facts[s_id])

  # Arrange all facts { 'key' : { server_id : value ... } }
  facts_by_key = defaultdict(dict)
  for server_id in facts:
    for key in facts[server_id]:
      facts_by_key[key][server_id] = facts[server_id][key]


  # We insert all computed facts with a bulk-insert per-key.
  for key in facts_by_key:
    servers_with_computed_fact = set(facts_by_key[key].keys())

    # To avoid inserting for every individual fact and retrying
    # with an update if the fact already exists, compute a list of
    # facts already present and call update for those via bulk_update.
    #
    # This query returns only one fact per server because the key is
    # unique.
    #
    # TODO(cvicentiu) This could be optimized by creating a unique key
    # for each ComputedServerFact (server_id, key) and relying on
    # the database to "ignore" updates.
    # This optimization only works for server facts that do not change
    # over the lifespan of the server.
    facts_already_in_db = (ComputedServerFact.objects
        .filter(server__id__in=servers_with_computed_fact,
                key=key))

    facts_in_db_by_s_id = {}
    for fact in facts_already_in_db:
      facts_in_db_by_s_id[fact.server.id] = fact

    # In order to not do an insert for every individual fact, compute
    # a list of facts that need to be updated and a list of facts
    # that need to be created.
    facts_create = []
    facts_update = []
    for s_id in servers_with_computed_fact:
      fact_value = facts_by_key[key][s_id]

      if s_id in facts_in_db_by_s_id:
        facts_in_db_by_s_id[s_id].value = fact_value
        facts_update.append(facts_in_db_by_s_id[s_id])
      else:
        computed_fact = ComputedServerFact(key=key, value=fact_value,
                                           server_id=s_id)
        facts_create.append(computed_fact)

    ComputedServerFact.objects.bulk_create(facts_create,
                                           batch_size=1000)
    ComputedServerFact.objects.bulk_update(facts_update, ['value'],
                                           batch_size=1000)


def check_if_upload_fact_exists(key : str, upload_id : int):
    try:
        return ComputedUploadFact.objects.get(key=key, upload_id=upload_id)
    except ComputedUploadFact.DoesNotExist:
        return None




def extract_upload_facts(start_date, end_date, data_extractors):
    servers = get_upload_data_for_data_extractors(start_date, end_date,
                                                  data_extractors)

    facts = defaultdict(dict)
    for extractor in data_extractors:
        new_facts = extractor.extract_facts(servers)
        for s_id in new_facts:
            facts[s_id].update(new_facts[s_id])

    facts_create = []
    facts_update = []
    for s_id in facts:
        for upload_id in facts[s_id]:
            for key in facts[s_id][upload_id]:
                fact_value = facts[s_id][upload_id][key]

                #TODO(cvicentiu) This is a rather slow check, it does one database
                # lookup per upload_id. This should be optimized for faster
                # processing.
                up_fact = check_if_upload_fact_exists(key, upload_id)
                if up_fact is None:
                    up_fact = ComputedUploadFact(key=key, value=fact_value,
                                                 upload_id=upload_id)
                    facts_create.append(up_fact)
                else:
                    up_fact.value = fact_value
                    facts_update.append(up_fact)

    ComputedUploadFact.objects.bulk_create(facts_create, batch_size=1000)
    ComputedUploadFact.objects.bulk_update(facts_update, ['value'],
                                           batch_size=1000)



# TODO: Not used atm
def add_os_srv_fact_if_missing(start_date, end_date):

  # Get the server list where the OS key exists
  servers_with_os = ComputedServerFact.objects.filter(
                      key='os_name').values_list(
                      'server_id',
                      flat=True)
  #print('srv_with_os', servers_with_os)
  servers_to_process = Server.objects.exclude(
                          id__in=servers_with_os)
  #print('srv_without_os', servers_to_process)

  update_list = []
  for srv in servers_to_process:

    fact = ComputedServerFact(server=srv,
                      key='os_name',
                      value='Linux')
    update_list.append(fact)

  ComputedServerFact.objects.bulk_create(update_list)


# TODO: Not used atm
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


