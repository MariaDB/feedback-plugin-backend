# pull official base image
ARG python_version=3.9
FROM "python:$python_version"

# set work directory
WORKDIR /app/

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1


RUN apt-get install apt-transport-https curl
RUN curl -o /etc/apt/trusted.gpg.d/mariadb_release_signing_key.asc 'https://mariadb.org/mariadb_release_signing_key.asc'
RUN sh -c "echo 'deb https://mirror.mariadb.org/repo/10.11/debian/ bookworm main' >>/etc/apt/sources.list"

# install gdal for django
RUN apt-get update && apt-get install libgdal-dev libmariadb-dev -y

# install dependencies
RUN pip install --upgrade pip && \
    pip install mariadb==1.1.6 gunicorn

COPY ./docker/app/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY ./docker/app/entrypoint.sh /entrypoint.sh

# logs for gunicorn
RUN mkdir -p ./logs/

WORKDIR /app/src/

# run entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
