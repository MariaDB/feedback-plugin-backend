ARG mariadb_version=10.6
FROM "mariadb:$mariadb_version"

RUN apt-get update && apt-get install -y python3 python3-pip libmariadb-dev
RUN pip install --upgrade pip && \
    pip install mariadb

COPY ./docker/mariadb/create_database_user.py \
     /docker-entrypoint-initdb.d/create_database_user.sh
RUN chmod +x /docker-entrypoint-initdb.d/create_database_user.sh
