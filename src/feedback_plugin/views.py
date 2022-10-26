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

import re
import csv
import logging

from django.db.models import (F, IntegerField, Count, Value,
                              DateField, Case, When, ProtectedError)
from django.db.models.functions import (Cast, ExtractYear, ExtractMonth,
                                        Concat, TruncYear, TruncMonth)
from django.http.response import (HttpResponse, HttpResponseNotAllowed,
                                  HttpResponseBadRequest, JsonResponse)
from django.contrib.gis.geoip2 import GeoIP2, GeoIP2Exception
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render

from geoip2.errors import GeoIP2Error


from .models import RawData
from .forms import UploadFileForm


from feedback_plugin.models import *

logger = logging.getLogger('views')

class ChartView(View):
  chart_id = None
  def get(self, request, *args, **kwargs):

    try:
      chart = Chart.objects.select_related('metadata').get(id=self.chart_id)
    except Chart.DoesNotExist:
      return JsonResponse({}) #No data


    return JsonResponse({
      'title' : chart.title,
      'values' : chart.values,
      'metadata' : chart.metadata
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
  pattern = re.compile('(?P<major>\d+).(?P<minor>\d+).(?P<point>\d+)')
  current_uploads_version = Data.objects.filter(name='VERSION',
                              upload__currentuploads__generated__isnull=False)

  for row in current_uploads_version:
    matches = pattern.match(row.value)
    version_string = "%s.%s.%s" % (
      matches.group('major'),
      matches.group('minor'),
      matches.group('point')
    )

    if version_string not in data:
      data[version_string] = 0

    data[version_string] = data[version_string] + 1

  sorted_items = [(k, v) for k, v in
    sorted(
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


@csrf_exempt
def file_post(request):

  if request.method != 'POST':
    return HttpResponseNotAllowed(['POST'])

  # Bind the file to the Django form
  form = UploadFileForm(request.POST, request.FILES)

  if not form.is_valid():
    return HttpResponseBadRequest()

  try:
    geoip = GeoIP2()
    if 'HTTP_X_REAL_IP' in request.META:
      ip = request.META['HTTP_X_REAL_IP']
    elif 'REMOTE_ADDRESS' in request.META:
      ip = request.META['REMOTE_ADDRESS']
    elif 'HTTP_X_FORWARDED_FOR' in request.META:
      ip = request.META['HTTP_X_FORWARDED_FOR'].partition(',')[0]
    else:
      ip = None
    report_country = geoip.country_code(ip)
  except (GeoIP2Exception, GeoIP2Error, TypeError) as e:
    report_country = 'ZZ' # Unknown according to ISO 3166-1993

  #TODO(andreia) configure web server to limit post size otherwise
  # we could run into a Denial of Service attack if we get too big of
  # an upload.
  data_upload = RawData(country=report_country,
                        data=request.FILES['data'].read())
  data_upload.save()

  response = HttpResponse("<h1>ok</h1>", status=200)
  return response
