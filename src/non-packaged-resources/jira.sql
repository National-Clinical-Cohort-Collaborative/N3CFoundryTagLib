create table raw_jira(id text, key text, fields jsonb);

select
	id,
	key,
	(fields->>'summary')::text as summary,
	(fields->>'created')::timestamp as created,
	(fields->>'updated')::timestamp as updated,
	(((fields->>'priority')::jsonb)->>'name')::text as priority,
	t.seqnum,
	jsonb_pretty(t.field::jsonb)
--	jsonb_pretty((((fields->>'description')::jsonb)->>'content')::jsonb) as content
from raw_jira
cross join lateral
    jsonb_array_elements_text((((fields->>'description')::jsonb)->>'content')::jsonb) with ordinality as t(field,seqnum)
;

create table n3c_admin.enclave_external_dataset (
	    id int,
	    key text,
	    summary text,
	    created timestamp,
	    updated timestamp,
	    priority text,
	    name text,
	    description text,
	    author_name text,
	    author_orcid text,
	    author_url text,
	    author_affiliation text,
	    license text,
	    cost text,
	    url text,
	    identifier text,
	    contain_geo_codes text,
	    contain_phi text,
	    domain_team_relevance text,
	    justification text,
	    contact text,
	    documentation text,
	    keywords text,
	    citation_name text,
	    citation_doi text,
	    citation_url text
);

create view palantir.enclave_external_dataset as select * from n3c_admin.enclave_external_dataset;
grant select on all tables in schema palantir to palantir;
