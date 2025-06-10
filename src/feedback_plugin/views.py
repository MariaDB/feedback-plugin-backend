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
import socket

from django.http.response import (HttpResponse, HttpResponseNotAllowed,
                                  HttpResponseBadRequest, JsonResponse,
                                  HttpResponseForbidden)
from django.contrib.gis.geoip2 import GeoIP2, GeoIP2Exception
from django.utils import timezone
from django.views import View
from django.views.decorators.csrf import csrf_exempt

from geoip2.errors import GeoIP2Error

from .models import Chart, Config, RawData, Upload, ComputedServerFact
from .forms import UploadFileForm


logger = logging.getLogger('views')


# Class based view to return chart data as a JSON response, based on chart ID.
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

        metadata = chart.metadata
        return JsonResponse({
            'title': chart.title,
            'values': chart.values,
            'metadata': {
                'computed_start_date': metadata.computed_start_date,
                'computed_end_date': metadata.computed_end_date,
            }
        })


# This is the endpoint that the MariaDB Feedback Plugin uses to post data.
# We do not do any active processing, only save the raw upload for later
# analysis.
def handle_upload_form(request, ip=None, upload_time=None):
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
        if ip is None:
            raise TypeError
        report_country = geoip.country_code(ip)
    except (GeoIP2Exception, GeoIP2Error, TypeError, socket.gaierror):
        report_country = 'ZZ'  # Unknown according to ISO 3166-1993

    if upload_time is None:
        upload_time = timezone.now()

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


# This is a special endpoint used to populate the database with data from a
# specific IP. The specific IP is passed as a HTTP header via
# HTTP_X_REPORT_FROM_IP. It is not used by the MariaDB Server plugin directly.
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


def get_uploads(request):
    proposed_key = request.GET.get('X_API_KEY_UPLOADS')
    if not proposed_key:
        return HttpResponseForbidden("Invalid API Key")

    if request.GET.get('X_API_KEY_UPLOADS') != Config.objects.get(key='X_API_KEY_UPLOADS').value:
        return HttpResponseForbidden("Invalid API Key")

    date = request.GET.get('date')

    if not date:
        return HttpResponseBadRequest("Missing date parameter")

    try:
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        return HttpResponseBadRequest("Invalid date format, expected YYYY-MM-DD")

    page = request.GET.get('page', 1)

    if not page.isdigit() or int(page) < 1:
        return HttpResponseBadRequest("Invalid page number, must be a positive integer")

    MAX_PER_PAGE = 100
    offset = (int(page) - 1) * MAX_PER_PAGE
    # Fetch data and uploads for the given date, paginated

    uploads_data = []
    uploads = (Upload.objects.select_related('server')
               .prefetch_related('data_set')
               .filter(upload_time__date=date)
               .order_by('id')[offset:offset + MAX_PER_PAGE])

    server_ids = set(upload.server.id for upload in uploads)

    facts = (ComputedServerFact.objects
             .select_related('server')
             .filter(server__in=server_ids)
             .filter(key='country_code'))
    server_dict = {fact.server.id: fact.value for fact in facts}

    for upload in uploads:
        data_list = []
        for data_item in upload.data_set.all():
            data_list.append({
                "key": data_item.key,
                "value": data_item.value
            })
        uploads_data.append({
            "id": upload.id,
            "data": data_list,
            "upload_time": upload.upload_time.isoformat(),
            "server": {
                "id": upload.server.id,
                "country_code": server_dict[upload.server.id],
            }
        })

    return JsonResponse({"uploads": uploads_data, "page": page}, safe=True)
