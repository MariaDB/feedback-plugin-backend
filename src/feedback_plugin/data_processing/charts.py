from datetime import datetime
from collections import defaultdict

from django.db.models.functions import ExtractYear, ExtractMonth
from django.db.models import Count
from django.db.models.expressions import RawSQL
from django.db import connection

from feedback_plugin.models import Upload
from .extractors import COLLECTED_FEATURES


def get_uploads(start_date: datetime, end_date: datetime, start_closed_interval: bool):
    '''
        Return the uploads in the provided time interval, grouped by year and
        month.
    '''

    uploads = Upload.objects.annotate(
        year=ExtractYear('upload_time'),
        month=ExtractMonth('upload_time'),
    ).values(
        'year',
        'month',
    )

    if start_closed_interval:
        uploads = uploads.filter(upload_time__gte=start_date)
    else:
        uploads = uploads.filter(upload_time__gt=start_date)

    return uploads.filter(
        upload_time__lte=end_date,
    )


def compute_server_count_by_month(start_date: datetime,
                                  end_date: datetime,
                                  start_closed_interval: bool
) -> dict[str, list[str]]:
    uploads = get_uploads(start_date, end_date, start_closed_interval)
    server_counts = uploads.annotate(
        count=Count('server__id', distinct=True),
    ).order_by(
        'year',
        'month',
    )

    return {
        'count': {
            'x': [f"{value['year']}-{value['month']:0>2}" for value in server_counts],
            'y': [int(f"{value['count']}") for value in server_counts]
        }
    }


def compute_feature_count_by_month(start_date: datetime,
                                   end_date: datetime,
                                   start_closed_interval: bool,
                                   feature: str,
) -> dict[str, dict[str, list[str] | list[int]]]:
    uploads = get_uploads(start_date, end_date, start_closed_interval)

    feature_query = """
    SELECT
        cuf.upload_id
    FROM
        feedback_plugin_computeduploadfact cuf
    WHERE
        cuf.key = 'features'
        AND JSON_VALUE(cuf.value, CONCAT('$.', %s))
    """
    server_counts = uploads.filter(
        id__in=RawSQL(feature_query, (feature,))
    ).annotate(
        count=Count('server__id', distinct=True)
    ).order_by(
        'year',
        'month',
    )

    return {
        feature: {
            'x': [f"{value['year']}-{value['month']:0>2}" for value in server_counts],
            'y': [int(f"{value['count']}") for value in server_counts]
        }
    }


def compute_feature_counts_by_month(start_date: datetime,
                                    end_date: datetime,
                                    start_closed_interval: bool,
) -> dict[str, dict[str, list[str] | list[int]]]:
    result = {}
    for feature in COLLECTED_FEATURES:
        result.update(
            compute_feature_count_by_month(
                start_date,
                end_date,
                start_closed_interval,
                feature,
            )
        )
    return result


def compute_version_breakdown_by_month(start_date: datetime,
                                       end_date: datetime,
                                       start_closed_interval: bool
) -> dict[str, list[str]]:
    start_date_string = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    end_date_string = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    query = f"""
    SELECT
        count(distinct u.server_id) as cnt,
        EXTRACT(YEAR FROM u.upload_time) year,
        EXTRACT(MONTH FROM u.upload_time) month,
        cuf1.value as major,
        cuf2.value as minor
    FROM
        feedback_plugin_computeduploadfact cuf1 JOIN
        feedback_plugin_computeduploadfact cuf2
            ON cuf1.upload_id = cuf2.upload_id JOIN
        feedback_plugin_upload u ON u.id = cuf1.upload_id
    WHERE
        cuf1.`key` = 'server_version_major' AND
        cuf2.`key` = 'server_version_minor' AND
        u.upload_time
            {'>=' if start_closed_interval else '>'} '{start_date_string}' AND
        u.upload_time <= '{end_date_string}'
    GROUP BY year, month, major, minor"""

    cursor = connection.cursor()
    cursor.execute(query)

    result = defaultdict(lambda: {'x': [], 'y': []})
    for row in cursor.fetchall():
        (count, year, month, major, minor) = row
        result[f'{major}.{minor}']['x'].append(f'{year}-{month}')
        result[f'{major}.{minor}']['y'].append(int(count))

    return result


def compute_architecture_breakdown_by_month(start_date: datetime,
                                            end_date: datetime,
                                            start_closed_interval: bool
) -> dict[str, list[str]]:
    start_date_string = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    end_date_string = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    query = f"""
    SELECT
        count(distinct u.server_id) as cnt,
        EXTRACT(YEAR FROM u.upload_time) year,
        EXTRACT(MONTH FROM u.upload_time) month,
        csf1.value as architecture
    FROM
        feedback_plugin_upload u JOIN
        feedback_plugin_server s
            ON u.server_id = s.id JOIN
        feedback_plugin_computedserverfact csf1
            ON csf1.server_id = s.id
    WHERE
        csf1.`key` = 'hardware_architecture' AND
        u.upload_time
            {'>=' if start_closed_interval else '>'} '{start_date_string}' AND
        u.upload_time <= '{end_date_string}'
    GROUP BY year, month, architecture"""

    cursor = connection.cursor()
    cursor.execute(query)

    result = defaultdict(lambda: {'x': [], 'y': []})
    for row in cursor.fetchall():
        (count, year, month, architecture) = row
        result[f'{architecture}']['x'].append(f'{year}-{month}')
        result[f'{architecture}']['y'].append(int(count))

    return result
