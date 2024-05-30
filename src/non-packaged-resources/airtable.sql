create view airtable.clin_orgs as
select
	(raw->>'id')::text as id,
	(((raw->>'fields')::jsonb)->>'clinOrgId')::text as clinorgid,
	(((raw->>'fields')::jsonb)->>'clinOrgName')::text as clinorgname,
	(((raw->>'fields')::jsonb)->>'clinOrgUrls')::text as clinorgurls,
	(((raw->>'fields')::jsonb)->>'clinOrgtype')::text as clinorgtype,
	(((raw->>'fields')::jsonb)->>'clinOrgPiInst')::jsonb as clinorgpiinst,
	(raw->>'createdTime')::timestamp as createdtime
from airtable.clin_orgs_raw;

create view airtable.inst as
select
	(raw->>'id')::text as id,
	(((raw->>'fields')::jsonb)->>'institutionID')::text as institutionid,
	(((raw->>'fields')::jsonb)->>'institutionName')::text as institutionname,
	(raw->>'createdTime')::timestamp as createdtime
from airtable.inst_raw;

create view airtable.citizens as
select
	(raw->>'id')::text as id,
	(raw->'fields'->>'firstName')::text as first_name,
	(raw->'fields'->>'lastName')::text as last_name,
	(raw->'fields'->>'email')::text as email,
	(raw->'fields'->>'date')::date as date,
	(raw->'fields'->>'covidOrTenant')::text as type,
	(raw->'fields'->>'firstNameMinor')::text as first_name_minor,
	(raw->'fields'->>'lastNameMinor')::text as last_name_minor,
	(raw->'fields'->>'emailMinor')::text as email_minor,
	(raw->'fields'->>'notes')::text as notes
from airtable.citizens_raw;

create view airtable.contact as
select
	(raw->>'id')::text as id,
	(raw->'fields'->>'firstName')::text as first_name,
	(raw->'fields'->>'lastName')::text as last_name,
	(raw->'fields'->>'contactName')::text as contact_name,
	(raw->'fields'->>'contactEmail')::text as contact_email,
	jsonb_array_elements_text(raw->'fields'->'institutionID') as inst_id
from airtable.contacts_raw;

create view airtable.role as
select
	(raw->>'id')::text as id,
	(raw->'fields'->>'roleType')::text as role
from airtable.roles_raw;

create or replace view airtable.tenant_dua_master_staging as
select
	(raw->>'id')::text as id,
	raw->'fields'->'inst'->>0 as inst,
	raw->'fields'->'institutionID'->>0 as inst_id,
	(raw->'fields'->>'Institution Name')::text as inst_name,
	raw->'fields'->'clinicalDuaContacts'->>0 as contacts,
	raw->'fields'->'clinicalDuaSignatory'->>0 as signatory,
	(raw->'fields'->>'clinicalDuaDateExecuted')::date as executed
from airtable.agreements_raw
where raw->'fields'->>'clinicalDuaDateExecuted'::text is not null;

create or replace view airtable.tenant_dua_master as
select
	dms.inst_id as institutionid,
	inst_name as institutionname,
	foo.first_name as tenantduacontactfirst,
	foo.last_name as tenantduacontactlast,
	null as tenantduacontactrole,
	foo.contact_email as tenantduacontactemail,
	executed as tenantduaexecuted,
	bar.first_name as tenantinstitutionalsigneefirst,
	bar.last_name as tenantinstitutionalsigneelast,
	null as tenantinstitutionalsigneerole,
	bar.contact_email as tenantinstitutionalsigneeemail
from tenant_dua_master_staging as dms
left outer join contact as foo
on contacts = foo.id
left outer join contact as bar
on signatory = bar.id
;

create or replace view airtable.covid_dua_master_staging as
select
	(raw->>'id')::text as id,
	raw->'fields'->'inst'->>0 as inst,
	raw->'fields'->'institutionID'->>0 as inst_id,
	(raw->'fields'->>'Institution Name')::text as inst_name,
	raw->'fields'->'covidDuaContacts'->>0 as contacts,
	raw->'fields'->'covidDuaSignatory'->>0 as signatory,
	raw->'fields'->'covidDurContact'->>0 as dur_contact,
	(raw->'fields'->>'covidDuaDateExecuted')::date as executed
from airtable.agreements_raw
where raw->'fields'->>'covidDuaDateExecuted'::text is not null;

create or replace view airtable.covid_dua_master as
select distinct
	dms.inst_id as institutionid,
	inst_name as institutionname,
	executed as duaexecuted,
	foo.first_name as duacontactfirstname,
	foo.last_name as duacontactsurname,
	null as duacontactrole,
	foo.contact_email as duacontactemail,
	bar.first_name as signatoryfirst,
	bar.last_name as signatorylast,
	null as signatoryrole,
	bar.contact_email as signatoryemail,
	dur_contact as durcontact
from covid_dua_master_staging as dms
left outer join contact as foo
on contacts = foo.id
left outer join contact as bar
on signatory = bar.id
;
