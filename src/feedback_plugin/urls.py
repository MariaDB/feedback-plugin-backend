'''
   Copyright (c) 2022 MariaDB
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

from django.contrib import admin
from django.urls import path, re_path

from feedback_plugin import views

urlpatterns = [
   path('admin/', admin.site.urls),

   path('rest/v1/server-count/', views.feedback_server_count,
                              name='server_count'),
   path('rest/v1/version-breakdown/', views.feedback_version_breakdown,
                                      name='version_breakdown'),
   path('rest/v1/architecture/', views.feedback_architecture,
                                 name='architecture'),
   path('rest/v1/os/', views.feedback_os, name='os'),
   path('rest/v1/file-post/', views.file_post, name='file_post')
]
