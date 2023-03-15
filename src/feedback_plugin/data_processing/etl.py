from collections import defaultdict
from datetime import datetime, timedelta
from io import StringIO
import csv
import logging

from django.db.models import Q

from feedback_plugin.models import (ComputedServerFact, ComputedUploadFact,
                                    Data, RawData, Server, Upload)
from .extractors import DataExtractor


logger = logging.getLogger('etl')


def process_from_date(start_date: datetime, end_date: datetime):
    raw_objects_iterator = RawData.objects.filter(
        upload_time__gt=start_date,
        upload_time__lte=end_date
    ).order_by('upload_time').iterator()

    for raw_upload in raw_objects_iterator:
        country_code = raw_upload.country
        raw_upload_time = raw_upload.upload_time

        raw_data = StringIO(raw_upload.data.decode('utf-8')
                            .replace('\x00', ''))

        reader = csv.reader(raw_data, delimiter='\t')

        values = []
        data = {}
        for row in reader:
            # We only expect KV pairs.
            if len(row) != 2:
                raw_upload.delete()
                continue

            data[row[0]] = row[1]
            values.append(Data(key=row[0], value=row[1]))

        if ('FEEDBACK_SERVER_UID' not in data
                or len(data['FEEDBACK_SERVER_UID']) == 0):
            raw_upload.delete()
            continue

        if ('FEEDBACK_USER_INFO' in data
                and data['FEEDBACK_USER_INFO'] == 'mysql-test'):
            raw_upload.delete()
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
            # We have a fact, but not a server attached to it. This should
            # never happen.
            assert False

        srv_facts = []

        def get_fact_by_key(key, server, value):
            try:
                srv_fact = ComputedServerFact.objects.get(key=key,
                                                          server=server)
                srv_fact.value = value
                return (False, srv_fact)
            except ComputedServerFact.DoesNotExist:
                return (True, ComputedServerFact(key=key,
                                                 value=value,
                                                 server=server))

        srv_facts.append(get_fact_by_key('country_code', server, country_code))
        srv_facts.append(get_fact_by_key('last_seen', server, raw_upload_time))
        srv_facts.append(get_fact_by_key('first_seen', server, raw_upload_time))
        # If first_seen already existed, we don't override it.
        if (srv_facts[-1][0] is False):
            srv_facts.pop()

        srv_facts_create = map(lambda x: x[1],
                               filter(lambda x: x[0], srv_facts))
        srv_facts_update = map(lambda x: x[1],
                               filter(lambda x: not x[0], srv_facts))

        # Create Server computed facts for this RawData.
        ComputedServerFact.objects.bulk_create(srv_facts_create,
                                               batch_size=1000)
        ComputedServerFact.objects.bulk_update(srv_facts_update, ['value'],
                                               batch_size=1000)

        # Create Upload and all Data entries for this RawData.
        upload = Upload(upload_time=raw_upload_time, server=server)
        upload.save()

        for value in values:
            value.upload = upload
        Data.objects.bulk_create(values)

        raw_upload.delete()


def process_raw_data():
    first_object = RawData.objects.order_by('upload_time').first()
    last_object = RawData.objects.order_by('-upload_time').first()

    if first_object is None:
        return  # Nothing to do

    start_date = first_object.upload_time - timedelta(seconds=1)
    end_date = last_object.upload_time

    total_days = end_date - start_date

    logger.info(f'Will process raw data for a total of {total_days}')

    slice_24_hours = 60 * 60 * 24
    while start_date <= end_date:
        local_start_date = start_date
        local_end_date = start_date + timedelta(seconds=slice_24_hours)
        logger.info(f'Processing from {start_date.strftime("%Y-%m-%d")} to '
                    f'{local_end_date.strftime("%Y-%m-%d")}')

        process_from_date(local_start_date, local_end_date)
        start_date = local_end_date

    logger.info('Finished processing data')


def get_upload_data_for_data_extractors(start_date: datetime,
                                        end_date: datetime,
                                        data_extractors: list[DataExtractor],
                                        end_inclusive: bool):
    keys = set()
    for extractor in data_extractors:
        keys |= extractor.get_required_keys()
    key_filter = Q()
    for key in keys:
        key_filter |= Q(key__iexact=key)

    date_filter = Q(upload__upload_time__gte=start_date)
    if end_inclusive:
        date_filter &= Q(upload__upload_time__lte=end_date)
    else:
        date_filter &= Q(upload__upload_time__lt=end_date)

    # Extract server data about Architecture and OS
    data_to_process = Data.objects.filter(
        date_filter & key_filter
    ).select_related('upload__server')

    servers = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for data in data_to_process:
        # In case there are multiple entries for the same server, use the
        # latest one only
        server_id = data.upload.server.id
        upload_id = data.upload.id
        # Appending to a list allows for multiple values for the same key.
        servers[server_id][upload_id][data.key.lower()].append(data.value)

    return servers


def extract_server_facts(start_date: datetime,
                         end_date: datetime,
                         data_extractors: DataExtractor,
                         end_inclusive: bool = True):
    servers = get_upload_data_for_data_extractors(start_date, end_date,
                                                  data_extractors,
                                                  end_inclusive)
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
        facts_already_in_db = ComputedServerFact.objects.filter(
            server__id__in=servers_with_computed_fact,
            key=key)

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


def check_if_upload_fact_exists(key: str,
                                upload_id: int) -> ComputedUploadFact | None:
    try:
        return ComputedUploadFact.objects.get(key=key, upload_id=upload_id)
    except ComputedUploadFact.DoesNotExist:
        return None


def extract_upload_facts(start_date: datetime,
                         end_date: datetime,
                         data_extractors: DataExtractor,
                         end_inclusive: bool = True):
    servers = get_upload_data_for_data_extractors(start_date, end_date,
                                                  data_extractors,
                                                  end_inclusive)

    facts = defaultdict(dict)
    for extractor in data_extractors:
        new_facts = extractor.extract_facts(servers)
        for s_id in new_facts:
            facts[s_id].update(new_facts[s_id])

    logger.info(f'Extracted facts for {len(facts)} servers')

    facts_create = []
    facts_update = []
    for s_id in facts:
        for upload_id in facts[s_id]:
            for key in facts[s_id][upload_id]:
                fact_value = facts[s_id][upload_id][key]

                # TODO(cvicentiu) This is a rather slow check, it does one
                # database lookup per upload_id. This should be optimized for
                # faster processing.
                up_fact = check_if_upload_fact_exists(key, upload_id)
                if up_fact is None:
                    up_fact = ComputedUploadFact(key=key, value=fact_value,
                                                 upload_id=upload_id)
                    facts_create.append(up_fact)
                else:
                    up_fact.value = fact_value
                    facts_update.append(up_fact)

    logger.info(f'Creating {len(facts_create)} new facts')
    ComputedUploadFact.objects.bulk_create(facts_create, batch_size=1000)
    logger.info(f'Updating {len(facts_update)} already existing facts')
    ComputedUploadFact.objects.bulk_update(facts_update, ['value'],
                                           batch_size=1000)
