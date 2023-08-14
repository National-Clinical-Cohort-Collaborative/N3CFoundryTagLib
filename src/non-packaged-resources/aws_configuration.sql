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


------- Drupal to STRAPI migration
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
create server aws foreign data wrapper postgres_fdw options(host 'drupal-prod-rds-pgsql.c3z6aos3kjza.us-east-1.rds.amazonaws.com',port '5432',dbname 'covid_cd2h');
create user mapping for eichmann server aws options(user 'drupal',password 'drupal');
create schema aws_drupal;
import foreign schema public from server aws into aws_drupal;
create schema drupal;

create view drupal.press_release_staging as
select
	nid,
	vid,
	title,
	body_value as description,
	body_summary as summary,
	field_photo_target_id,
	field_photo_alt,
	field_date_value,
	field_a_uri as url
from (select nid,vid,title from aws_drupal.node_field_data where type='press_release' and status=1) as foo
LEFT JOIN
	aws_drupal.node__body
ON  foo.nid = node__body.entity_id
LEFT JOIN
    aws_drupal.node__field_photo
ON  foo.nid = node__field_photo.entity_id
LEFT JOIN
    aws_drupal.node__field_date
ON  foo.nid = node__field_date.entity_id
LEFT JOIN
    aws_drupal.node__field_a
ON  foo.nid = node__field_a.entity_id
;

create view drupal.webinar_staging as
select
	nid,
	vid,
	title,
	body_value as description,
	body_summary as summary,
	field_webinar_url_value as url,
	field_recording_date_value as recording_date,
	field_video_topic_value as topic
from (select nid,vid,title from aws_drupal.node_field_data where type='webinar' and status=1) as foo
LEFT JOIN
    aws_drupal.node__body
ON  foo.nid = node__body.entity_id
LEFT JOIN
    aws_drupal.node__field_webinar_url
ON  foo.nid = node__field_webinar_url.entity_id
LEFT JOIN
    aws_drupal.node__field_recording_date
ON  foo.nid = node__field_recording_date.entity_id
LEFT JOIN
    aws_drupal.node__field_video_topic
ON  foo.nid = node__field_video_topic.entity_id
;

insert into strapi.you_tube_videos(header,url,width,height,footer,created_at,updated_at,published_at,created_by_id,updated_by_id)
select
	title,
	url,
	427,240,
	topic,
	now(),now(),now(),
	1,1
from drupal.webinar
;

insert into strapi.press_releases(title,description,date,url,created_at,updated_at,published_at,created_by_id,updated_by_id)
select
	title,
	description,
	field_date_value::date,
	url,
	now(),now(),now(),
	1,1
from drupal.press_release
;

select id,title,date from strapi.press_releases;
select nid,title,field_date_value::date as date from drupal.press_release;
select id as fid,nid,filename,uri,field_photo_alt from aws_drupal.file_managed,drupal.press_release,strapi.files
where fid=field_photo_target_id and filename=name;

select id,nid,foo.title,date from strapi.press_releases as foo, drupal.press_release as bar
where 
foo.title = bar.title
and foo.date = bar.field_date_value::date
;

insert into files_related_morphs(file_id,related_id,related_type,field,"order")
select
	fid,
	id,
	'api::press-release.press-release' as type,
	'image' as field,
	1 as order
from
(select id as fid,nid,filename,uri,field_photo_alt from aws_drupal.file_managed,drupal.press_release,strapi.files
where fid=field_photo_target_id and filename=name) as foo2
,
(select id,nid,foo.title,date from strapi.press_releases as foo, drupal.press_release as bar
where 
foo.title = bar.title
and foo.date = bar.field_date_value::date
) as bar2
where foo2.nid=bar2.nid
;

update strapi.files set alternative_text = (select distinct field_photo_alt from 
(select distinct filename,field_photo_alt from aws_drupal.file_managed,drupal.press_release where fid=field_photo_target_id ) as foo
where files.name = filename limit 1)
;
