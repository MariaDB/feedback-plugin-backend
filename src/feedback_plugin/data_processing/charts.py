from datetime import datetime
from collections import defaultdict

from django.db.models.functions import ExtractYear, ExtractMonth
from django.db.models import Count
from django.db import connection

from feedback_plugin.models import Upload


def compute_server_count_by_month(start_date: datetime,
                                  end_date: datetime,
                                  start_closed_interval: bool
) -> dict[str, list[str]]:
    server_counts = Upload.objects.annotate(
        year=ExtractYear('upload_time'),
        month=ExtractMonth('upload_time'),
    ).values(
        'year',
        'month',
    ).annotate(
        count=Count('server__id', distinct=True),
    )

    if start_closed_interval:
        server_counts = server_counts.filter(upload_time__gte=start_date)
    else:
        server_counts = server_counts.filter(upload_time__gt=start_date)

    server_counts = server_counts.filter(
        upload_time__lte=end_date,
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


def compute_version_breakdown_by_month(start_date: datetime,
                                       end_date: datetime,
                                       start_closed_interval: bool
) -> dict[str, list[str]]:
    start_date_string = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    end_date_string = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    query = f"""
    SELECT
        count(*) as cnt,
        EXTRACT(YEAR FROM u.upload_time) year,
        EXTRACT(MONTH FROM u.upload_time) month,
        cuf1.value as major,
        cuf2.value as minor
    FROM
        feedback_plugin_computeduploadfact cuf1 JOIN
        feedback_plugin_computeduploadfact cuf2
            ON cuf1.upload_id = cuf2.upload_id JOIN
        feedback_plugin_upload u ON u.id = cuf1.upload_id JOIN
        feedback_plugin_server s ON u.server_id = s.id
    WHERE
        cuf1.key = 'server_version_major' AND
        cuf2.key = 'server_version_minor' AND
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
