create materialized view enclave_concept.code_set_concept as
select
	codeset_id,
	(t.item::jsonb->>'isExcluded')::boolean as is_excluded,
	(t.item::jsonb->>'includeMapped')::boolean as include_mapped,
	(t.item::jsonb->>'includeDescendants')::boolean as include_descendants,
	(t.item::jsonb->>'concept')::jsonb->>'DOMAIN_ID' as domain_id,
	((t.item::jsonb->>'concept')::jsonb->>'CONCEPT_ID')::int as concept_id,
	(t.item::jsonb->>'concept')::jsonb->>'CONCEPT_CODE' as concept_code,
	(t.item::jsonb->>'concept')::jsonb->>'CONCEPT_NAME' as concept_name,
	(t.item::jsonb->>'concept')::jsonb->>'VOCABULARY_ID' as vocabulary_id,
	(t.item::jsonb->>'concept')::jsonb->>'INVALID_REASON' as invalid_reason,
	(t.item::jsonb->>'concept')::jsonb->>'CONCEPT_CLASS_ID' as concept_class_id,
	(t.item::jsonb->>'concept')::jsonb->>'STANDARD_CONCEPT' as standard_concept,
	(t.item::jsonb->>'concept')::jsonb->>'INVALID_REASON_CAPTION' as invalid_reason_caption,
	(t.item::jsonb->>'concept')::jsonb->>'STANDARD_CONCEPT_CAPTION' as standard_concept_caption,
	jsonb_pretty(t.item::jsonb)
from enclave_concept.code_sets
cross join lateral
    jsonb_array_elements_text((((regexp_replace(atlas_json,'\n',' ','g')::jsonb->>'expression')::jsonb)->>'items')::jsonb) with ordinality as t(item,seqnum)
where atlas_json is not null;

create view enclave_concept.concept_set as
select
	foo.*,
	provisional_approval_date,
	release_name
from
	(select
		codeset_id,
		alias,
		code_sets.intention,
		version,
		is_most_recent_version,
		update_message,
		code_sets.created_by,
		limitations,
		issues,
		provenance,
		jsonb_pretty(regexp_replace(atlas_json,'\n',' ','g')::jsonb) as json
	from
		enclave_concept.code_sets,
		enclave_concept.concept_set_container_edited
	where code_sets.concept_set_name = concept_set_container_edited.concept_set_id
	  and concept_set_container_edited.status != 'Under Construction'
	  and is_most_recent_version
	) as foo
left outer join
	enclave_concept.provisional_approvals
on (codeset_id=concept_set_id)
;

create view enclave_concept.concept_set_display as
select
	concept_set.*,
	first_name,
	last_name,
	name
from
	enclave_concept.concept_set
	left outer join (select * from
		n3c_admin.user_binding,
		n3c_admin.registration
		where user_binding.orcid_id = registration.orcid_id) as foo
on concept_set.created_by = foo.unite_user_id
	left outer join
		n3c_admin.user_binding_fix
on concept_set.created_by = user_binding_fix.unite_user_id
;

create view enclave_concept.concept_set_project as
select
	concept_set.codeset_id,
	alias,
	research_project_id,
	title
from
	enclave_concept.concept_set
natural join
	enclave_concept.concept_set_to_research_project_edited
join
	n3c_admin.enclave_project
on (research_project_id= uid)
;

create table zenodo_deposit_raw(codeset_id int, raw jsonb);

create view zenodo_deposit as
select
	codeset_id,
	(raw->>'id')::int as zenodo_id,
	raw->>'title' as title,
	(raw->>'created')::date as created,
	(raw->>'modified')::date as modified,
	raw->>'submitted' as submitted,
	((raw->>'links')::jsonb)->>'bucket' as bucket,
	((raw->>'links')::jsonb)->>'publish' as publish,
	'https://doi.org/'||(((((raw->>'metadata')::jsonb)->>'prereserve_doi')::jsonb)->>'doi') as doi
from zenodo_deposit_raw
;

create table zenodo_file_raw(codeset_id int, raw jsonb, suffix text);

create view zenodo_file as
select
	codeset_id,
	(raw->>'created')::date as created,
	(raw->>'updated')::date as updated,
	((raw->>'links')::jsonb)->>'self' as url,
	suffix
from zenodo_file_raw
;

create table zenodo_published (
	codeset_id int,
	published timestamp
	);
	
	