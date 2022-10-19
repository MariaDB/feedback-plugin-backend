# pull official base image
ARG python_version=3.9
FROM "python:$python_version"

# set work directory
WORKDIR /app/

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install gdal
RUN apt-get update && apt-get install libgdal-dev -y

# install dependencies
RUN pip install --upgrade pip && \
    pip install mariadb==1.0.11 gunicorn

COPY ./docker/app/requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY ./docker/app/entrypoint.sh ./

# copy project
RUN mkdir -p ./logs/

WORKDIR /app/src/

# run entrypoint.sh
ENTRYPOINT ["/app/entrypoint.sh"]
