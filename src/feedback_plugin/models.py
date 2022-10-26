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
from django.db import models
from django_countries.fields import CountryField

from django.utils import timezone


class RawData(models.Model):
  '''
    This table holds the raw upload data reported.
  '''
  country = CountryField()
  data = models.BinaryField()
  upload_time = models.DateTimeField(default=timezone.now)

  def __str__(self):
    return f'{self.country}, {self.upload_time}, {len(self.data)}'



class Server(models.Model):
  '''
    This table holds an entry for each unique server that has reported
    data to the feedback plugin.
  '''
  pass


class Upload(models.Model):
  '''
    This table represents a data upload done by a particular server.
  '''
  upload_time = models.DateTimeField()
  server = models.ForeignKey('Server',
    on_delete=models.PROTECT,
    db_column='server_id'
  )

  class Meta:
    indexes = [
        models.Index(fields=['upload_time', 'server_id'])
    ]


  def __str__(self):
    return f'{self.upload_time}, {self.server.id}'


class Data(models.Model):
  '''
    This table holds the raw data uploaded by a server.
  '''
  key = models.CharField(max_length=100)
  value = models.CharField(max_length=1000)
  upload = models.ForeignKey('Upload',
    on_delete=models.PROTECT,
    db_column='upload_id'
  )

  def __str__(self):
    return f'{{{self.key} : {self.value}}} '


class ComputedUploadFact(models.Model):
  '''
    This table holds computed metadata for each upload.
  '''
  upload = models.ForeignKey('Upload',
    on_delete=models.PROTECT,
    db_column='upload_id'
  )
  key = models.CharField(max_length=100)
  value = models.CharField(max_length=1000)


class ComputedServerFact(models.Model):
  '''
    This table holds computed metadata for each server.
  '''
  server = models.ForeignKey('Server',
    on_delete=models.PROTECT,
    db_column='server_id'
  )
  key = models.CharField(max_length=100)
  value = models.CharField(max_length=1000)

  def __str__(self):
    return f'{self.server.id} -> {self.name} = {self.value}'


class Chart(models.Model):
  '''
    This table holds the pre-computed feedback plugin data used for charts
    generation.
  '''
  id = models.SlugField(max_length=100, primary_key=True)
  title = models.CharField(max_length=250) #Pretty title
  values = models.JSONField(default=dict, blank=True, null=False)


  class Meta:
    verbose_name_plural = "Feedback Plugin Chart Results"

  def __str__(self):
    return self.title

class ChartMetadata(models.Model):
  chart = models.OneToOneField('Chart', primary_key=True, on_delete=models.CASCADE,
                               related_name='metadata')
  computed_start_date = models.DateTimeField(blank=True, null=True)
  computed_end_date = models.DateTimeField(blank=True, null=True)
