create view n3c_admin.public_health_question as
select
	nid,
	vid,
	title,
	field_analysis_plan_research_met_value as analysis_plan,
	field_application_close_date_value as application_close_date,
	field_application_open_date_value as application_open_date,
	field_award_value as award,
	field_contact_name_value as contact_name,
	field_contact_email_value as contact_email,
	field_study_aims_value as study_aims,
	field_description_value as description,
	field_deliverables_value as deliverables,
	field_exclusion_criteria_value as exclusion_criteria,
	field_expected_results_value as expected_results,
	field_id_value as id,
	field_inclusion_criteria_value as inclusion_criteria,
	field_length_of_study_value as length_of_study,
	field_number_of_awards_value as number_of_awards,
	field_owner_value as owner,
	field_phenotype_value as phenotype,
	field_post_date_value as post_date,
	field_priority_value as priority,
	to_timestamp(node_field_data.created::numeric::double precision) AS created,
	to_timestamp(node_field_data.changed::numeric::double precision) AS changed
from public.node_field_data
left join public.node__field_award on (nid=public.node__field_award.entity_id)
left join public.node__field_analysis_plan_research_met on (nid=public.node__field_analysis_plan_research_met.entity_id)
left join public.node__field_application_close_date on (nid=public.node__field_application_close_date.entity_id)
left join public.node__field_application_open_date on (nid=public.node__field_application_open_date.entity_id)
left join public.node__field_contact_name on (nid=public.node__field_contact_name.entity_id)
left join public.node__field_contact_email on (nid=public.node__field_contact_email.entity_id)
left join public.node__field_study_aims on (nid=public.node__field_study_aims.entity_id)
left join public.node__field_description on (nid=public.node__field_description.entity_id)
left join public.node__field_deliverables on (nid=public.node__field_deliverables.entity_id)
left join public.node__field_exclusion_criteria on (nid=public.node__field_exclusion_criteria.entity_id)
left join public.node__field_expected_results on (nid=public.node__field_expected_results.entity_id)
left join public.node__field_id on (nid=public.node__field_id.entity_id)
left join public.node__field_inclusion_criteria on (nid=public.node__field_inclusion_criteria.entity_id)
left join public.node__field_length_of_study on (nid=public.node__field_length_of_study.entity_id)
left join public.node__field_number_of_awards on (nid=public.node__field_number_of_awards.entity_id)
left join public.node__field_owner on (nid=public.node__field_owner.entity_id)
left join public.node__field_phenotype on (nid=public.node__field_phenotype.entity_id)
left join public.node__field_post_date on (nid=public.node__field_post_date.entity_id)
left join public.node__field_priority on (nid=public.node__field_priority.entity_id)
where type='public_health_question'
;

create table n3c_web.public_health_question (
	nid int,
	vid int,
	title text,
	analysis_plan text,
	application_close_date date,
	application_open_date date,
	award int,
	contact_name text,
	contact_email text,
	study_aims text,
	description text,
	deliverables text,
	exclusion_criteria text,
	expected_results text,
	id int,
	inclusion_criteria text,
	length_of_study text,
	number_of_awards int,
	owner text,
	phenotype text,
	post_date date,
	priority text,
	created timestamp,
	changed timestamp
	);
