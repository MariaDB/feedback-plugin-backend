#!/bin/sh

echo 'Booting up web-app'
if ! python manage.py check --database default; then
  sleep 2;
  echo 'Database not available, exiting.'
  return 1;
fi

if !(python manage.py migrate &&
     python manage.py collectstatic --no-input --clear); then
  sleep 5;
  return 1;
fi

exec "$@"
