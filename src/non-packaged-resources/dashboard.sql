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

create view n3c_dashboard.severity_by_zip_staging as
select * from
(select distinct zip from n3c_questions.group_by_zip_censored) as f0
natural left outer join
(select zip,num_patients as unaffected from n3c_questions.group_by_zip_censored where severity_type='Unaffected') as f1
natural left outer join
(select zip,num_patients as mild from n3c_questions.group_by_zip_censored where severity_type='Mild') as f2
natural left outer join
(select zip,num_patients as mild_ed from n3c_questions.group_by_zip_censored where severity_type='Mild_ED') as f3
natural left outer join
(select zip,num_patients as moderate from n3c_questions.group_by_zip_censored where severity_type='Moderate') as f4
natural left outer join
(select zip,num_patients as severe from n3c_questions.group_by_zip_censored where severity_type='Severe') as f5
natural left outer join
(select zip,num_patients as mortality from n3c_questions.group_by_zip_censored where severity_type='Dead_w_COVID') as f6
;

create view n3c_dashboard.zip_full as
select * from
(select zip,zip as full_zip,irs_estimated_population as est_pop from n3c_dashboard.zip_code where zip >=10000) as f0
natural join
(select zip::int,unaffected,mild,mild_ed,moderate,severe,mortality from n3c_dashboard.severity_by_zip_staging where length(zip) = 5) as f1
;

create view n3c_dashboard.zip_four as
select * from
(select zip/10 as zip,zip as full_zip,irs_estimated_population as est_pop from n3c_dashboard.zip_code where zip >=10000) as f0
natural join
(select zip::int,unaffected,mild,mild_ed,moderate,severe,mortality from n3c_dashboard.severity_by_zip_staging where length(zip) = 4) as f1
where full_zip not in (select zip from zip_full)
;

create view n3c_dashboard.zip_three as
select * from
(select zip/100 as zip,zip as full_zip,irs_estimated_population as est_pop from n3c_dashboard.zip_code where zip >=10000) as f0
natural join
(select zip::int,unaffected,mild,mild_ed,moderate,severe,mortality from n3c_dashboard.severity_by_zip_staging where length(zip) = 3) as f1
where full_zip not in (select zip from zip_full)
  and full_zip not in (select zip from zip_four)
;

create view n3c_dashboard.severity_by_zip as
select * from n3c_dashboard.zip_full
union
select * from n3c_dashboard.zip_four
union
select * from n3c_dashboard.zip_three
;
