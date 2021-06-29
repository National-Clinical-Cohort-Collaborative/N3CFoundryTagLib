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

