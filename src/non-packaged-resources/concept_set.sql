create view code_set_staging as select codeset_id,regexp_replace(atlas_json,'[\r\n]',' ','g')::jsonb as atlas_json from code_sets;

create materialized view enclave_concept.code_set_concept as
select
	codeset_id,
	(item->>'isExcluded')::boolean as is_excluded,
	(item->>'includeMapped')::boolean as include_mapped,
	(item->>'includeDescendants')::boolean as include_descendants,
	(item->>'concept')::jsonb->>'DOMAIN_ID' as domain_id,
	((item->>'concept')::jsonb->>'CONCEPT_ID')::int as concept_id,
	(item->>'concept')::jsonb->>'CONCEPT_CODE' as concept_code,
	(item->>'concept')::jsonb->>'CONCEPT_NAME' as concept_name,
	(item->>'concept')::jsonb->>'VOCABULARY_ID' as vocabulary_id,
	(item->>'concept')::jsonb->>'INVALID_REASON' as invalid_reason,
	(item->>'concept')::jsonb->>'CONCEPT_CLASS_ID' as concept_class_id,
	(item->>'concept')::jsonb->>'STANDARD_CONCEPT' as standard_concept,
	(item->>'concept')::jsonb->>'INVALID_REASON_CAPTION' as invalid_reason_caption,
	(item->>'concept')::jsonb->>'STANDARD_CONCEPT_CAPTION' as standard_concept_caption,
	jsonb_pretty((item->>'concept')::jsonb) as concept
from code_set_staging
cross join lateral
	jsonb_array_elements((atlas_json->>'items')::jsonb) with ordinality as t(item,seqnum)
;

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
    jsonb_array_elements_text((((regexp_replace(atlas_json,'[\r\n]',' ','g')::jsonb->>'expression')::jsonb)->>'items')::jsonb) with ordinality as t(item,seqnum)
where atlas_json is not null;

create view enclave_concept.concept_set as
select
	'recommended' as set_type,
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
		code_sets.omop_vocab_version,
		limitations,
		issues,
		provenance,
		jsonb_pretty(regexp_replace(atlas_json,'\n',' ','g')::jsonb) as json
	from
		enclave_concept.code_sets,
		enclave_concept.concept_set_container_edited
	where code_sets.concept_set_name = concept_set_container_edited.concept_set_id
	) as foo,
	enclave_concept.provisional_approvals
	where codeset_id=concept_set_id
union
select
	'user' as set_type,
	foo.*,
	date_submitted_zenodo as provisional_approval_date,
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
		code_sets.omop_vocab_version,
		limitations,
		issues,
		provenance,
		jsonb_pretty(regexp_replace(atlas_json,'\n',' ','g')::jsonb) as json
	from
		enclave_concept.code_sets,
		enclave_concept.concept_set_container_edited
	where code_sets.concept_set_name = concept_set_container_edited.concept_set_id
	) as foo,
	enclave_concept.user_submitted_concept_sets_for_zenodo
	where codeset_id=concept_set_id
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

create view enclave_concept.concept_json_staging as
select
		concept_id,
		concept_name,
		domain_id,
		vocabulary_id,
		concept_class_id,
		coalesce(standard_concept, '') as standard_concept,
		coalesce(standard_concept_caption, '') as standard_concept_caption,
		concept_code,
		coalesce(invalid_reason, 'V') as invalid_reason,
		coalesce(invalid_reason_caption, 'Valid') as invalid_reason_caption
	from enclave_concept.concept
	natural left join enclave_concept.standard_concept_map
	natural left join enclave_concept.invalid_reason_map
;

create view enclave_concept.concept_json_staging2 as
select
	codeset_id,
	json_build_object(
		'isexcluded',isexcluded,
		'includedescendants',includedescendants,
		'includemapped',includemapped,
		'concept',to_jsonb(concept_json_staging)
		) as item
from concept_json_staging natural join concept_set_version_item_rv_edited
;

create view enclave_concept.concept_json as
select
	codeset_id,
	jsonb_pretty(json_build_object(
		'id', codeset_id,
		'name', alias,
		'expression', json_build_object('items',(select json_agg(item) from enclave_concept.concept_json_staging2 as foo where foo.codeset_id = concept_set_display.codeset_id))
	)::jsonb) as json
from concept_set_display
;

create table enclave_concept.zenodo_deposition_full_raw(raw jsonb);

create view enclave_concept.zenodo_deposition_full as
select
	(raw->>'links')::jsonb->>'doi' as doi,
	(raw->>'links')::jsonb->>'parent_doi' as concept_doi,
	(raw->>'metadata')::jsonb->>'title' as title
from enclave_concept.zenodo_deposition_full_raw
;

create view enclave_concept.zenodo_doi_map as
select codeset_id,zenodo_deposit.doi,concept_doi from zenodo_deposit,zenodo_deposition_full where zenodo_deposit.doi=zenodo_deposition_full.doi
union
select codeset_id,zenodo_published.version_doi,concept_doi from zenodo_published,zenodo_deposition_full where zenodo_published.version_doi=zenodo_deposition_full.doi
;
