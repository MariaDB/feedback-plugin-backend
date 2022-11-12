from datetime import datetime

from django.db.models.functions import ExtractYear, ExtractMonth
from django.db.models import Count

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
        'x': [f"{value['year']}-{value['month']:0>2}" for value in server_counts],
        'y': [int(f"{value['count']}") for value in server_counts]
    }

def compute_version_breakdown_by_month(start_date: datetime,
                                       end_date: datetime,
                                       start_closed_interval: bool
) -> dict[str, list[str]]:

    #TODO(cvicentiu) Implement this
    return {
        'x' : [],
        'y' : []
    }
