CREATE EXTENSION IF NOT EXISTS postgres_fdw;
create server aws foreign data wrapper postgres_fdw options(host 'drupal-prod-rds-pgsql.c3z6aos3kjza.us-east-1.rds.amazonaws.com',port '5432',dbname 'cd2h');
create user mapping for eichmann server aws options(user 'drupal',password '');
create schema aws_n3c_admin;
import foreign schema n3c_admin from server aws into aws_n3c_admin;
