create view enclave_cohort.comorbidity_view_full as
	select * from
	(select
		positive.variable,
		positive.mild as mild_positive,
		positive.mild + negative.mild as mild_total,
		to_char((positive.mild * 100.0) / (positive.mild + negative.mild),'990D99%') as mild_percentage,
		positive.mild_ed as mild_ed_positive,
		positive.mild_ed + negative.mild_ed as mild_ed_total,
		to_char((positive.mild_ed * 100.0) / (positive.mild_ed + negative.mild_ed),'990D99%') as mild_ed_percentage	,
		positive.moderate as moderate_positive,
		positive.moderate + negative.moderate as moderate_total,
		to_char((positive.moderate * 100.0) / (positive.moderate + negative.moderate),'990D99%') as moderate_percentage,
		positive.severe as severe_positive,
		positive.severe + negative.severe as severe_total,
		to_char((positive.severe * 100.0) / (positive.severe + negative.severe),'990D99%') as severe_percentage,
		positive.dead_w_covid as dead_w_covid_positive,
		positive.dead_w_covid + negative.dead_w_covid as dead_w_covid_total,
		to_char((positive.dead_w_covid * 100.0) / (positive.dead_w_covid + negative.dead_w_covid),'990D99%') as dead_w_covid_percentage
	from
	(select * from enclave_cohort.xport__charlson_uncollapsed_frequency where value='true') as positive,
	(select * from enclave_cohort.xport__charlson_uncollapsed_frequency where value is null) as negative
	where positive.variable = negative.variable) as foo
natural join
	(select
		positive.variable,
		positive.positive_sum as all_sum,
		positive.positive_sum + negative.negative_sum as all_total,
		to_char((positive.positive_sum * 100.0) / (positive.positive_sum + negative.negative_sum),'990D99%') as all_percentage
	from
	(select variable,mild+mild_ed+moderate+severe+dead_w_covid as positive_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value='true') as positive
	natural join
	(select variable,mild+mild_ed+moderate+severe+dead_w_covid as negative_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value is null) as negative
	) as bar
natural join
	(select
		positive.variable,
		positive.positive_sum as hospital_sum,
		positive.positive_sum + negative.negative_sum as hospital_total,
		to_char((positive.positive_sum * 100.0) / (positive.positive_sum + negative.negative_sum),'990D99%') as hospital_percentage
	from
	(select variable,moderate+severe+dead_w_covid as positive_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value='true') as positive
	natural join
	(select variable,moderate+severe+dead_w_covid as negative_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value is null) as negative
	) as done
;

create view comorbidity_view as
select variable, mild_percentage as mild, mild_ed_percentage as mild_id, moderate_percentage as moderate, severe_percentage as severe, dead_w_covid_percentage as dead_w_covid, all_percentage as all, hospital_percentage as hospital from comorbidity_view_full 
union
select 'Totals' as variable,'(n = '||mild||')','(n = '||mild_ed||')','(n = '||moderate||')','(n = '||severe||')','(n = '||dead_w_covid||')','(n = '||all_cnt||')','(n = '||hospital||')'
	from (select variable,sum(mild) as mild,sum(mild_ed) as mild_ed,sum(moderate) as moderate,sum(severe) as severe,sum(dead_w_covid) as dead_w_covid,sum(mild+mild_ed+moderate+severe+dead_w_covid) as all_cnt,sum(moderate+severe+dead_w_covid) as hospital from xport__charlson_uncollapsed_frequency group by 1 limit 1) as foo
;
