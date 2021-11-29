create schema n3c_dashboard;

create materialized view n3c_dashboard.aggregated as
select
	race,
	ethnicity,
	age_bin,
	gender_concept_name as gender,
	severity_type as severity,
	num_patients,
	case when num_patients='<20' then 0 else num_patients::int end as patient_count,
	total_patients,
	case when total_patients='<20' then 0 else total_patients::int end as patient_total
from n3c_questions.final_results_with_gender_censored ;
