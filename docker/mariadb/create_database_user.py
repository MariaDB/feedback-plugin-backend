#!/usr/bin/python3

import mariadb
import os
import sys
import time

try:
  conn = mariadb.connect(
    user='root',
    password=os.environ['MARIADB_ROOT_PASSWORD'],
    host='localhost',
    port=3306)
except mariadb.Error as e:
  print(f'Error connection to MariaDB {e}, retrying...')
  print(e)
  sys.exit(1)

print('Succesfully connected to MariaDB')

cur = conn.cursor()
print('Creating database...')
sql = "create database if not exists {}".format(
        os.environ['DJANGO_DB_NAME'])
cur.execute(sql)


print('Creating Django user...')
sql = "create user if not exists `{}` identified by '{}'".format(
        os.environ['DJANGO_DB_USER_NAME'],
        os.environ['DJANGO_DB_USER_PASSWORD'])
cur.execute(sql)

print('Granting user rights on Django database...')
sql = "grant all on `{}`.* to `{}`".format(
         os.environ['DJANGO_DB_NAME'],
         os.environ['DJANGO_DB_USER_NAME'])
cur.execute(sql)

print('Granting rights for running Django tests...')
sql = "grant create on *.* to `{}`".format(
         os.environ['DJANGO_DB_USER_NAME'])
cur.execute(sql)

sql = "grant all on `test_{}`.* to `{}`".format(
         os.environ['DJANGO_DB_NAME'],
         os.environ['DJANGO_DB_USER_NAME'])
cur.execute(sql)

conn.close()
