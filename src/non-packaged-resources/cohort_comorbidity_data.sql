select
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
where positive.variable = negative.variable;

select
	positive.variable,
	positive.positive_sum,
	positive.positive_sum + negative.negative_sum as sum_total,
	to_char((positive.positive_sum * 100.0) / (positive.positive_sum + negative.negative_sum),'990D99%') as sum_percentage
from
(select variable,mild+mild_ed+moderate+severe+dead_w_covid as positive_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value='true') as positive
natural join
(select variable,mild+mild_ed+moderate+severe+dead_w_covid as negative_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value is null) as negative
;

select
	positive.variable,
	positive.positive_sum,
	positive.positive_sum + negative.negative_sum as sum_total,
	to_char((positive.positive_sum * 100.0) / (positive.positive_sum + negative.negative_sum),'990D99%') as sum_percentage
from
(select variable,moderate+severe+dead_w_covid as positive_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value='true') as positive
natural join
(select variable,moderate+severe+dead_w_covid as negative_sum from enclave_cohort.xport__charlson_uncollapsed_frequency where value is null) as negative
;
