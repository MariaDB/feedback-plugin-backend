'''
   Copyright (c) 2022 MariaDB Foundation

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA
'''

import datetime
import logging
import re
import socket

from django.db.models import (F, Count, Value, DateField, Case, When)
from django.db.models.functions import (ExtractYear, ExtractMonth, TruncMonth)
from django.http.response import (HttpResponse, HttpResponseNotAllowed,
                                  HttpResponseBadRequest, JsonResponse,
                                  HttpResponseForbidden)
from django.contrib.gis.geoip2 import GeoIP2, GeoIP2Exception
from django.utils import timezone
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from geoip2.errors import GeoIP2Error


from .models import Chart, Config, Data, RawData, Upload
from .forms import UploadFileForm


logger = logging.getLogger('views')


class ChartView(View):
    chart_id = None

    def get(self, request, *args, **kwargs):
        try:
            chart = Chart.objects.select_related(
                'metadata'
            ).get(
                id=self.chart_id
            )
        except Chart.DoesNotExist:
            return JsonResponse({})  # No data

        return JsonResponse({
            'title': chart.title,
            'values': chart.values,
            'metadata': chart.metadata
        })


def feedback_server_count(request):
    query = Upload.objects.annotate(
        up_year=ExtractYear('uploaded'),
        up_month=ExtractMonth('uploaded'),
        time_period=TruncMonth('uploaded', output_field=DateField())
    ).values(
        'up_year',
        'up_month',
        'time_period',
    ).annotate(
        server_count=Count('server_id', distinct=True)
    ).order_by(
        'time_period'
    )

    if not query.exists():
        print('No data was found to be updated.')

    return JsonResponse({'result': list(query)})


def feedback_version_breakdown(request):
    data = {}
    pattern = re.compile('(?P<major>\\d+).(?P<minor>\\d+).(?P<point>\\d+)')
    current_uploads_version = Data.objects.filter(
        name='VERSION',
        upload__currentuploads__generated__isnull=False)

    for row in current_uploads_version:
        matches = pattern.match(row.value)
        version = f"{matches.group('major')}." \
                  f"{matches.group('minor')}." \
                  f"{matches.group('point')}"

        if version not in data:
            data[version] = 0

        data[version] = data[version] + 1

    sorted_items = [(k, v) for k, v in sorted(
            data.items(),
            key=lambda item: item[1],
            reverse=True
        )][:20]
    result = dict(sorted_items)

    return JsonResponse({'result': result})


def feedback_architecture(request):
    query = Data.objects.filter(
        name='uname_machine'
    ).select_related(
        'upload__currentuploads__upload'
    ).annotate(
        arch=Case(
            When(value='x64', then=Value('x86_64')),
            default=F('value')
        )
    ).values(
        'arch'
    ).annotate(
        server_count=Count('id')
    ).order_by('-server_count')

    return JsonResponse({'result': list(query)})


def feedback_os(request) -> JsonResponse:
    query = Data.objects.filter(
        name='uname_sysname'
    ).select_related('upload__currentuploads').values(
        'value'
    ).annotate(
        server_count=Count('id')
    ).order_by('-server_count')

    return JsonResponse({'result': list(query)})


def handle_upload_form(request, ip=None, upload_time=timezone.now()):
    if request.method != 'POST':
        return HttpResponseNotAllowed(['POST'])

    # Bind the file to the Django form
    form = UploadFileForm(request.POST, request.FILES)

    if not form.is_valid():
        return HttpResponseBadRequest()

    report_country = 'ZZ'
    try:
        geoip = GeoIP2()
        if ip is None:
            if 'HTTP_X_REAL_IP' in request.META:
                ip = request.META['HTTP_X_REAL_IP']
            elif 'REMOTE_ADDRESS' in request.META:
                ip = request.META['REMOTE_ADDRESS']
            elif 'HTTP_X_FORWARDED_FOR' in request.META:
                ip = request.META['HTTP_X_FORWARDED_FOR'].partition(',')[0]
        report_country = geoip.country_code(ip)
    except (GeoIP2Exception, GeoIP2Error, TypeError, socket.gaierror):
        report_country = 'ZZ'  # Unknown according to ISO 3166-1993

    # TODO(andreia) configure web server to limit post size otherwise
    # we could run into a Denial of Service attack if we get too big of
    # an upload.
    data_upload = RawData(country=report_country,
                          data=request.FILES['data'].read(),
                          upload_time=upload_time)
    data_upload.save()

    response = HttpResponse("<h1>ok</h1>", status=200)
    return response


@csrf_exempt
def file_post(request):
    return handle_upload_form(request)


@csrf_exempt
def file_post_with_ip(request):
    try:
        config = Config.objects.get(key='X_API_KEY')
    except Config.DoesNotExist:
        # Server misconfigured.
        return HttpResponse('No X_API_KEY configured for Server', status=403)

    if ('HTTP_X_API_KEY' not in request.META
            or request.META['HTTP_X_API_KEY'] != config.value):
        return HttpResponseForbidden()

    ip = request.META['HTTP_X_REPORT_FROM_IP']
    date = datetime.datetime.strptime(request.META['HTTP_X_REPORT_DATE'],
                                      '%Y-%m-%d %H:%M:%S.%f')

    upload_time = date.replace(tzinfo=datetime.timezone.utc)

    return handle_upload_form(request, ip, upload_time)
