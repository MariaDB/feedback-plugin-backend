#! /bin/bash
echo "*** Initializing user ***"
echo "create user if not exists '$MARIADB_USER'@localhost identified by '$MARIADB_PASSWORD'"| \
 mariadb -uroot -p$MARIADB_ROOT_PASSWORD

echo "*** Initializing DB ***"
echo "drop database if exists $MARIADB_DATABASE; create database $MARIADB_DATABASE;" |\
 mariadb -uroot -p$MARIADB_ROOT_PASSWORD

echo "*** Initializing test DB ***"
echo "drop database if exists $DJANGO_TEST_DB_NAME; create database $DJANGO_TEST_DB_NAME;" |\
 mariadb -uroot -p$MARIADB_ROOT_PASSWORD

echo "*** Grant privileges to user on DB ***"
echo "grant all privileges on $MARIADB_DATABASE.* to $MARIADB_USER@localhost" | \
  mariadb -uroot -p$MARIADB_ROOT_PASSWORD

 echo "*** Grant privileges to user on test DB ***"
echo "grant all privileges on $DJANGO_TEST_DB_NAME.* to $MARIADB_USER@localhost" | \
 mariadb -uroot -p$MARIADB_ROOT_PASSWORD
