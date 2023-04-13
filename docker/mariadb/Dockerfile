ARG mariadb_version=10.6
FROM "mariadb:$mariadb_version"

RUN apt-get update
COPY ./docker/mariadb/initdb.sh \
     /docker-entrypoint-initdb.d/initdb.sh
RUN chmod +x /docker-entrypoint-initdb.d/initdb.sh
