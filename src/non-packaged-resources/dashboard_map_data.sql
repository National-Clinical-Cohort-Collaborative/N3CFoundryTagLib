create table enclave_cohort.map_region(id int, name text);

create table enclave_cohort.map_state(id int, name text, abbrev text);

create view enclave_cohort.region_frequency as
select id,name,sum(num_patients) from atients_subregion_for_map,map_region where name=subregion group by 1,2 order by 3;



insert into enclave_cohort.map_region(name,id) values('New England',1);
insert into enclave_cohort.map_region(name,id) values('West South Central',2);
insert into enclave_cohort.map_region(name,id) values('Pacific',3);
insert into enclave_cohort.map_region(name,id) values('Middle Atlantic',4);
insert into enclave_cohort.map_region(name,id) values('East South Central',5);
insert into enclave_cohort.map_region(name,id) values('Mountain',6);
insert into enclave_cohort.map_region(name,id) values('West North Central',7);
insert into enclave_cohort.map_region(name,id) values('East North Central',8);
insert into enclave_cohort.map_region(name,id) values('South Atlantic',9);

insert into enclave_cohort.map_state(name,abbrev,id) values( 'Alabama', 'AL', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Arizona',  'AZ', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Arkansas', 'AR', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'California',  'CA', 3);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Colorado', 'CO', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Connecticut', 'CT', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Delaware',  'DE', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Florida', 'FL', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Georgia',  'GA', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Idaho', 'ID', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Illinois',  'IL', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Indiana', 'IN', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Iowa', 'IA', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Kansas',  'KS', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Kentucky',  'KY', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Louisiana',  'LA', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Maine',  'ME', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Maryland', 'MD', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Massachusetts', 'MA', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Michigan', 'MI', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Minnesota',  'MN', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Mississippi', 'MS', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Missouri', 'MO', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Montana', 'MT', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Nebraska', 'NE', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Nevada', 'NV', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New Hampshire', 'NH', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New Jersey',  'NJ', 4);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New Mexico', 'NM', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New York', 'NY', 4);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'North Carolina', 'NC', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'North Dakota', 'ND', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Ohio', 'OH', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Oklahoma',  'OK', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Oregon', 'OR', 3);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Pennsylvania', 'PA', 4);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Rhode Island', 'RI', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'South Carolina',  'SC', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'South Dakota',  'SD', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Tennessee',  'TN', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Texas',  'TX', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Utah', 'UT', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Vermont', 'VT', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Virginia', 'VA', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Washington',  'WA', 3);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'West Virginia', 'WV', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values(  'Wisconsin',  'WI', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Wyoming', 'WY', 6);

create view enclave_cohort.site_staging_new as 
	select institutionid,institutionname,latitude,longitude
	from n3c_admin.dta_master,ror.address
	where dtaexecuted is not null and institutionid=id
union
	select institutionid,institutionname,latitude,longitude
	from n3c_admin.dta_master,enclave_cohort.map_backfill
	where dtaexecuted is not null and institutionid=id
;

create view enclave_cohort.site_staging_new2 as 
select foo.*,datamodelfinal,submitted,released
from
	enclave_cohort.site_staging_new as foo
natural left join
	enclave_cohort.data_acquisition_site_tracker
;


create view enclave_cohort.map_sites_new as
select
	site_staging_new2.institutionid as id,
	site_staging_new2.institutionname as site,
	'http://'||institutionhomepage as url,
	regexp_replace(clinorgtype, ' .*', '') as type,
	case
		when datamodelfinal = 'ACT' or datamodelfinal = 'OMOP' or datamodelfinal = 'PCORnet' or datamodelfinal = 'TNX' then datamodelfinal
		else 'pending'
	end as data_model,
	case
		when lower(submitted) ~ 'y' and lower(released) ~ 'y' then 'available'
		when lower(submitted) ~ 'y' then 'submitted'
		else 'pending'
	end as status,
	latitude,
	longitude
from
enclave_cohort.site_staging_new2 left outer join enclave_cohort.institution_master
on site_staging_new2.institutionid = institution_master.institutionid
;

---------------------------------------------

create view n3c_maps.feed_master as 
select
	institutionid as ror_id,
	institutionname as name,
	case
		when clinorgtype is null then 'Unaffiliated'
		else clinorgtype
	end as type,
	gov::boolean,
	nih::boolean,
	'https://'||usabledomain as url,
	latitude,
	longitude
from enclave_cohort.institution_master, ror.address
where institutionid != ''
  and institutionid = id
  and institutionid not in (select ror_id from location_non_ror)
union
select
	institutionid as ror_id,
	institutionname as name,
	case
		when clinorgtype is null then 'Unaffiliated'
		else clinorgtype
	end as type,
	gov::boolean,
	nih::boolean,
	'https://'||usabledomain as url,
	latitude,
	longitude
from enclave_cohort.institution_master, location_non_ror
where institutionid != ''
  and institutionid = ror_id
union
select
	institutionid as ror_id,
	institutionname as name,
	case
		when clinorgtype is null then 'Unaffiliated'
		else clinorgtype
	end as type,
	gov::boolean,
	nih::boolean,
	'https://'||usabledomain as url,
	0 as latitude,
	0 as longitude
from enclave_cohort.institution_master
where institutionid != ''
  and institutionid not in (select id from ror.address)
  and institutionid not in (select ror_id from location_non_ror)
union
select distinct
	ror_id,
	organization.name,
	'Unaffiliated' as type,
	false as gov,
	false as nih,
	link as url,
	latitude,
	longitude
from scholar_profile.authorship_map, ror.organization, ror.address, ror.link
where authorship_map.ror_id = organization.id
  and organization.id = address.id
  and organization.id = link.id
  and link.seqnum = 1
  and ror_id not in (select institutionid from enclave_cohort.institution_master where institutionid is not null)
union
select distinct
	authorship_map.ror_id,
	location_non_ror.name,
	'Unaffiliated' as type,
	false as gov,
	false as nih,
	'https://'||authorship_map.ror_id as url,
	latitude,
	longitude
from scholar_profile.authorship_map, location_non_ror
where authorship_map.ror_id = location_non_ror.ror_id
  and authorship_map.ror_id != 'http'
  and authorship_map.ror_id not in (select institutionid from enclave_cohort.institution_master where institutionid is not null)
;

create view staging_dta_available as
select
	ror_id,
	name,
	url,
	regexp_replace(type, ' .*', '') as type,
	cdm as data_model,
	case
		when processed_in_foundry and released then 'available'
		when processed_in_foundry then 'submitted'
		else 'pending'
	end as status,
	latitude,
	longitude
from feed_master,site_mapping,site_status_for_export
where feed_master.ror_id=site_mapping.ror
  and site_mapping.abbreviation=site_status_for_export.abbreviation
;

create view staging_dta_pending as
select
	ror_id,
	name,
	url,
	regexp_replace(type, ' .*', '') as type,
	'pending' as data_model,
	'pending' as status,
	latitude,
	longitude
from feed_master 
where ror_id in (select institutionid from n3c_admin.dta_master where dtaexecuted is not null)
  and ror_id not in (select ror_id from staging_dta_available)
;

create materialized view contributing_sites as
select * from staging_dta_available
union
select * from staging_dta_pending
;

--------------------------------------------

-- publications-map.jsp (Inter-institutional Publications Map)
--		overview/publication_map/publication_map.jsp
--			modules/publication_map.jsp
--				modules/collaboration_map_code.jsp
--					feeds/sitePublications.jsp
--						n3c_collaboration.publication_organization
--							publication_organization_class
--								scholar_profile.authorship
--								scholar_profile.authorship_map
--								ror.organization
--								organization_class
--							ror.address (lat/long)
--							location_non_ror (lat/long)
--					feeds/sitePublicationEdges.jsp
--						n3c_collaboration.publication_edge
--							scholar_profile.authorship
--							scholar_profile.authorship_map
--					feeds/sitePublicationLegend.jsp
--						n3c_collaboration.publication_organization
--						n3c_dashboard.site_map (dimension definition)

-- collaboration.jsp (Inter-institutional Collaboration Map)
--		overview/collaboration_map/collaboration_map.jsp
--			modules/collaboration_map.jsp
--				modules/collaboration_map_code.jsp
--					feeds/siteCollaborations.jsp
--						n3c_collaboration.collaboration_organization
--							organization_organization
--								palantir.n3c_user
--								organization_class
--									enclave_cohort.institution_master
--								n3c_admin.enclave_project_members
--							ror.address (lat/long)
--							location_non_ror (lat/long)
--					feeds/siteCollaborationEdges.jsp
--						n3c_collaboration.collaboration_edge
--							n3c_admin.enclave_project_members
--							palantir.n3c_user
--							organization_organization
--							organization_project
--					feeds/siteCollaborationLegend.jsp
--						n3c_collaboration.collaboration_organization
--						n3c_dashboard.site_map (dimension definition)

-- contributing-sites.jsp (Institutions Contributing Data)
--		overview/contributing_sites/contributing_sites.jsp
--			modules/site_map.jsp
--				modules/site_map_code.jsp
--					feeds/siteLocations.jsp
--						n3c_maps.sites

-- new_ph/severity_region/map1.jsp (Regional Distribution of COVID+ Patients)
--		severity_region/feeds/regions.jsp
--			n3c_questions_new.cases_by_severity_by_state_censored_regional_distribution
--			n3c_dashboard.state_map
--			n3c_dashboard.severity_map
--			n3c_maps.sites
--				staging_enclave
--					site_mapping
--					ror.address (lat/long)
--					site_status_for_export
--					enclave_cohort.institution_master
--				staging_pending
--					n3c_admin.dta_master
--					ror.address (lat/long)
--					enclave_cohort.institution_master
--				staging_no_ror
--					n3c_admin.dta_master
--					site_mapping
--					ror.address (lat/long)
--					enclave_cohort.institution_master
--			ror.address

create schema n3c_maps;

create table n3c_maps.site_mapping (
	abbreviation text,
	ror text
	);

create table site_status_for_export (
	processed_in_foundry boolean,
	released boolean,
	site_name text,
	abbreviation text,
	sftp_folder_name text,
	cdm text
	);

SELECT
	site_mapping.abbreviation,
    site_mapping.ror,
    dta_master.institutionname,
    site_status_for_export.site_name
FROM
	site_mapping,
    n3c_admin.dta_master,
    site_status_for_export
WHERE site_status_for_export.abbreviation = site_mapping.abbreviation
  AND site_mapping.dta_ror = dta_master.institutionid
;

create view n3c_maps.staging_enclave as
select
	ror as id,
	case
		when site_name = 'uofcahealth' then institutionname
		else site_name
	end as site,
	'http://'||institutionhomepage as url,
	regexp_replace(clinorgtype, ' .*', '') as type,
	cdm as data_model,
	case
		when processed_in_foundry and released then 'available'
		when processed_in_foundry then 'submitted'
		else 'pending'
	end as status,
	latitude,
	longitude
from
	n3c_maps.site_mapping,
	ror.address,
	n3c_maps.site_status_for_export,
	enclave_cohort.institution_master
where ror=id
  and site_mapping.abbreviation=site_status_for_export.abbreviation
  and site_mapping.dta_ror = institution_master.institutionid
;

create view n3c_maps.staging_pending as
select
	dta_master.institutionid as id,
	dta_master.institutionname as site,
	'http://'||institutionhomepage as url,
	regexp_replace(clinorgtype, ' .*', '') as type,
	'pending' as data_model,
	'pending' as status,
	latitude,
	longitude
from
	n3c_admin.dta_master,
	ror.address,
	enclave_cohort.institution_master
where dta_master.institutionid not in (select dta_ror from n3c_maps.site_mapping)
  and dta_master.institutionid = institution_master.institutionid
  and dta_master.institutionid = address.id
  and dta_master.dtaexecuted is not null
;

create view n3c_maps.staging_no_ror as
select
	dta_master.institutionid as id,
	dta_master.institutionname as site,
	'http://'||institutionhomepage as url,
	regexp_replace(clinorgtype, ' .*', '') as type,
	'pending' as data_model,
	'pending' as status,
	latitude,
	longitude
from
	n3c_admin.dta_master,
	n3c_maps.site_mapping,
	ror.address,
	enclave_cohort.institution_master
where dta_master.institutionid not in (select dta_ror from n3c_maps.staging_enclave)
  and dta_master.institutionid not in (select dta_ror from n3c_maps.staging_pending)
  and dta_master.institutionid = site_mapping.ror
  and dta_master.dtaexecuted is not null
  and site_mapping.dta_ror = institution_master.institutionid
  and site_mapping.dta_ror = address.id
;

create view n3c_maps.sites as
select * from n3c_maps.staging_enclave
union
select * from n3c_maps.staging_pending
union
select * from n3c_maps.staging_no_ror
;
