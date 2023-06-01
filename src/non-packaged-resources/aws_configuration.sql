CREATE EXTENSION IF NOT EXISTS postgres_fdw;
create server aws foreign data wrapper postgres_fdw options(host 'drupal-prod-rds-pgsql.c3z6aos3kjza.us-east-1.rds.amazonaws.com',port '5432',dbname 'cd2h');
create user mapping for eichmann server aws options(user 'drupal',password '');
create schema aws_n3c_admin;
import foreign schema n3c_admin from server aws into aws_n3c_admin;
create schema aws_enclave_cohort;
import foreign schema enclave_cohort from server aws into aws_enclave_cohort;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;
create server deep_thought foreign data wrapper postgres_fdw options(host 'deep-thought.info-science.uiowa.edu',port '5432',dbname 'loki');
create user mapping for eichmann server aws options(user 'eichmann',password 'translational');
create schema deep_thought_pubmed_central;
import foreign schema pubmed_central from server deep_thought into deep_thought_pubmed_central;
create schema deep_thought_covid_pmc;
import foreign schema covid_pmc from server deep_thought into deep_thought_covid_pmc;
