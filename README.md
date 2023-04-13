# Feedback Plugin for MariaDB Server Backend

This project is the back-end used to collect and show data from MariaDB Server's
[feedback plugin](https://mariadb.com/kb/en/feedback-plugin/).

The software is licensed under GPLv2.

# Running the project
The backend is written in `Django`. The recommended way to deploy the project
is by using `docker-compose`.

## Dependencies
The host machine only needs `docker` and `docker-compose` installed. Follow your
distribution's instructions for [docker](https://docs.docker.com/desktop/linux/install/)
and for [docker-compose](https://docs.docker.com/compose/install/) to install them.

# Initial start-up
```
$ cd docker/
$ docker-compose up --build
```

The following set of commands will prepare the project for an optimal
development workflow.
Created containers:
```bash
$ docker ps
CONTAINER ID   IMAGE          COMMAND                  CREATED              STATUS                        PORTS                                   NAMES
1b498234aec9   nginx:latest   "/docker-entrypoint.…"   About a minute ago   Up 34 seconds                 0.0.0.0:8000->80/tcp, :::8000->80/tcp   prod-nginx
844dc37fad7d   docker-web     "/entrypoint.sh guni…"   About a minute ago   Up 35 seconds                                                         prod-web
f87577521801   docker-db      "docker-entrypoint.s…"   About a minute ago   Up About a minute (healthy)   3306/tcp                                prod-mariadb

```

Created images:
```bash
$ docker images
REPOSITORY                    TAG       IMAGE ID       CREATED              SIZE
docker-web                    latest    83c6ea637d2b   About a minute ago   1.45GB
docker-db                     latest    24e2d178147f   2 hours ago          436MB
nginx                         latest    6efc10a0510f   32 hours ago         142MB
```

Optionally if needed to remove database and run database initialization routine run:
```bash
$ cd docker/
$ sudo rm -rf mariadb/datadir
```

# Running tests
First make sure the container stack is functional, by running `docker ps`. You
should see the following entries:

```
84c2814a6500   docker_nginx   "nginx -g 'daemon of…"   26 hours ago   Up 26 hours   0.0.0.0:8000->80/tcp, :::8000->80/tcp   docker_nginx_1
c2d0bce78133   docker_web     "/app/entrypoint.sh …"   26 hours ago   Up 26 hours                                           docker_web_1
09e10ff5387a   docker_db      "docker-entrypoint.s…"   26 hours ago   Up 26 hours   3306/tcp                                docker_db_1
```

Tests are run from within the `web` container. The command to run all unit tests
is:

```
docker exec -it docker_web_1 python manage.py test
```

The server listens on port 8000 on the local machine. You can access it via:

http://127.0.0.1:8000

# Contributing
The MariaDB Foundation welcomes contributions to this project. Feel free to
submit a pull request via the regular GitHub workflow.

For a more in-depth code documentation, have a look at [CODING.md](CODING.md)
