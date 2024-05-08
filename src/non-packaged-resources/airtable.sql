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
