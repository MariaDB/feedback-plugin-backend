# pull official base image
ARG python_version=3.9
FROM "python:$python_version"

# set work directory
WORKDIR /app/src

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libgdal-dev

# install dependencies
COPY ./app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -U pip \
  && pip install --no-cache-dir -r /tmp/requirements.txt

COPY ./app/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

