create materialized view fhir.entry as
select
	(entry->>'resource')::jsonb as entry,
	((entry->>'resource')::jsonb)->>'resourceType' as type
from
	fhir.json_raw
cross join lateral
	jsonb_array_elements(((raw->>'entry')::jsonb)::jsonb) with ordinality as t(entry)
;

create index entry_type on entry(type);

synthea=# select type,count(*) from entry group by 1;
┌────────────────────┬──────────┐
│        type        │  count   │
├────────────────────┼──────────┤
│ AllergyIntolerance │   624235 │
│ CarePlan           │  2971284 │
│ Condition          │  5806396 │
│ DiagnosticReport   │  3715319 │
│ Encounter          │ 15100691 │
│ Goal               │  2264495 │
│ Immunization       │ 10406532 │
│ Medication         │  3860280 │
│ MedicationRequest  │  4779183 │
│ Observation        │ 57579428 │
│ Organization       │  1423390 │
│ Patient            │  1594095 │
│ Procedure          │  7497676 │
└────────────────────┴──────────┘
(13 rows)

----------------------------
-- 3.0

create materialized view fhir.allergy_intolerance as
select
	identifier,
	"clinicalStatus" as clinical_status,
	"verificationStatus" as verification_status,
	t.type,
	category, -- split out
	criticality,
	code, -- split out?
	substr((patient->>'reference'),10)::text as patient,
	"onsetDateTime" as onset_date_time,
	"onsetAge" as onset_age,
	"onsetPeriod" as onset_period,
	"onsetRange" as onset_range,
	"onsetString" as onset_string,
	"assertedDate" as asserted_date,
	recorder,
	asserter,
	"lastOccurrence" as last_occurence,
	note,
	reaction
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		identifier jsonb,
		"clinicalStatus" text,
		"verificationStatus" text,
		type text,
		category jsonb,
		criticality text,
		code jsonb,
		patient jsonb,
		"onsetDateTime" timestamp,
		"onsetAge" jsonb,
		"onsetPeriod" jsonb,
		"onsetRange" jsonb,
		"onsetString" text,
		"assertedDate" timestamp,
		recorder jsonb,
		asserter jsonb,
		"lastOccurrence" timestamp,
		note jsonb,
		reaction jsonb
	)
where
	entry.type='AllergyIntolerance'
;

-- this one is not deployed - not sure it adds anything
create materialized view fhir.allergy_intolerance_category as
select
	patient,
	jsonb_array_elements_text(category) as category
from
	fhir.allergy_intolerance
;

create materialized view fhir.allergy_intolerance_code as
select
	patient,
	t2.*
from
	fhir.allergy_intolerance,
	jsonb_to_record(code) as t(coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

----------------------------
-- 3.0
create materialized view fhir.care_plan as
select
	identifier,
	definition,
	"basedOn" as based_on,
	replaces,
	"partOf" as part_of,
	status,
	intent,
	category,
	title,
	description,
	substr((subject->>'reference'),10)::text as subject,
	substr((context->>'reference'),10)::text as context,
	(period->>'start')::date as period_start,
	(period->>'end')::date as period_end,
	author,
	"careTeam" as care_team,
	addresses,
	"supportingInfo" as supporting_info,
	goal,
	activity,
	note
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		identifier jsonb,
		definition jsonb,
		"basedOn" jsonb,
		replaces jsonb,
		"partOf" jsonb,
		status text,
		intent text,
		category jsonb,
		title text,
		description text,
		subject jsonb,
		context jsonb,
		period jsonb,
		author jsonb,
		"careTeam" jsonb,
		addresses jsonb,
		"supportingInfo" jsonb,
		goal jsonb,
		activity jsonb,
		note jsonb
	)
where
	entry.type='CarePlan'
;

create materialized view fhir.care_plan_category as
select
	subject,
	context,
	t2.*
from
	fhir.care_plan,
	jsonb_to_recordset(category) as t(coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

create materialized view fhir.care_plan_addresses as
select
	subject,
	context,
	substr(reference,10)::text as addresses
from
	fhir.care_plan,
	jsonb_to_recordset(addresses) as t2(reference text)
;

create materialized view fhir.care_plan_activity as
select
	subject,
	context,
	t1.status,
	t3.*
from
	fhir.care_plan,
	jsonb_to_recordset(activity) as t(detail jsonb),
	jsonb_to_record(detail) as t1(status text, code jsonb),
	jsonb_to_record(code) as t2(coding jsonb),
	jsonb_to_recordset(coding) as t3(code text, system text, display text)
;

----------------------------
-- 3.0
create materialized view fhir.condition as
select
	id,
	identifier,
	"clinicalStatus" as clinital_status,
	"verificationStatus" as verification_status,
	category,
	severity,
	code,
	"bodySite" as body_site,
	substr((subject->>'reference'),10)::text as subject,
	substr((context->>'reference'),10)::text as context,
	"onsetDateTime" as onset_date_time,
	"onsetAge" as onset_age,
	"onsetPeriod" as onset_period,
	"onsetRange" onset_range,
	"onsetString" onset_string,
	"abatementDateTime" as abatement_date_time,
	"abatementAge" as abatement_age,
	"abatementPeriod" as abatement_period,
	"abatementRange" as abatement_range,
	"abatementString" as abatement_string,
	"assertedDate" as asserted_date,
	asserter,
	stage,
	evidence,
	note
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		id text, -- present in Synthea
		identifier jsonb,
		"clinicalStatus" text,
		"verificationStatus" text,
		category jsonb,
		severity jsonb,
		code jsonb,
		"bodySite" jsonb,
		subject jsonb,
		context jsonb,
		"onsetDateTime" timestamp,
		"onsetAge" jsonb,
		"onsetPeriod" jsonb,
		"onsetRange" jsonb,
		"onsetString" text,
		"abatementDateTime" timestamp,
		"abatementAge" jsonb,
		"abatementPeriod" jsonb,
		"abatementRange" jsonb,
		"abatementString" text,
		"assertedDate" timestamp,
		asserter jsonb,
		stage jsonb,
		evidence jsonb,
		note jsonb
	)
where
	entry.type='Condition'
;

create materialized view fhir.condition_code as
select
	id,
	t2.*
from
	fhir.condition,
	jsonb_to_record(code) as t(coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

----------------------------
-- 3.0
create materialized view fhir.diagnostic_report as
select
	id,
	identifier,
	"basedOn" as based_on,
	status,
	category,
	code,
	substr((subject->>'reference'),10)::text as subject,
	substr((context->>'reference'),10)::text as context,
	"effectiveDateTime" as effective_date_time,
	"effectivePeriod" as effective_period,
	issued,
	performer,
	specimen,
	result,
	note,
	"imagingStudy" as image_study,
	image,
	conclusion,
	"codedDiagnosis" as coded_diagnosis,
	"presentedForm" as presented_form
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		id text, -- present in Synthea
		identifier jsonb,
		"basedOn" jsonb,
		status text,
		category jsonb,
		code jsonb,
		subject jsonb,
		context jsonb,
		"effectiveDateTime" timestamp,
		"effectivePeriod" jsonb,
		issued text,
		performer jsonb,
		specimen jsonb,
		result jsonb,
		note jsonb,
		"imagingStudy" jsonb,
		image jsonb,
		conclusion text,
		"codedDiagnosis" jsonb,
		"presentedForm" jsonb
	)
where
	entry.type='DiagnosticReport'
;

create materialized view fhir.diagnostic_report_code as
select
	id,
	t2.*
from
	fhir.diagnostic_report,
	jsonb_to_record(code) as t(coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

create materialized view fhir.diagnostic_report_result as
select
	id,
	display,
	substr(reference,10)::text as reference	
from
	fhir.diagnostic_report,
	jsonb_to_recordset(result) as t(display text, reference text)
;

----------------------------
-- 3.0
create materialized view fhir.encounter as
select
	id,
	identifier,
	status,
	"statusHistory" as status_history,
	(class->>'code')::text as class,
	"classHistory" as class_history,
	priority,
	t.type,
	"serviceType" as service_type,
	substr((subject->>'reference'),10)::text as subject,
	"episodeOfCare" as episode_of_care,
	"incomingReferral" as incoming_referral,
	participant,
	appointment,
	(period->>'start')::date as period_start,
	(period->>'end')::date as period_end,
	length,
	reason,
	diagnosis,
	account,
	hospitalization,
	location,
	substr(("serviceProvider"->>'reference'),10)::text as service_provider,
	"partOf" as part_of
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		id text, -- present in Synthea
		identifier jsonb,
		status text,
		"statusHistory" jsonb,
		class jsonb,
		"classHistory" jsonb,
		priority jsonb,
		type jsonb,
		"serviceType" jsonb,
		subject jsonb,
		"episodeOfCare" jsonb,
		"incomingReferral" jsonb,
		participant jsonb,
		appointment jsonb,
		period jsonb,
		length jsonb,
		reason jsonb,
		diagnosis jsonb,
		account jsonb,
		hospitalization jsonb,
		location jsonb,
		"serviceProvider" jsonb,
		"partOf" jsonb
	)
where
	entry.type='Encounter'
;

create materialized view fhir.encounter_type as
select
	id,
	text,
	t2.*
from
	fhir.encounter,
	jsonb_to_recordset(encounter.type) as t(text text, coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text)
;

create materialized view fhir.encounter_reason as
select
	id,
	t2.*
from
	fhir.encounter,
	jsonb_to_record(reason) as t(coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

----------------------------
-- 3.0
create materialized view fhir.goal as
select
	identifier,
	status,
	category,
	priority,
	(description->>'text')::text as description,
	subject,
	"startDate" as start_date,
	"startCodeableConcept" as start_codeable_concept,
	target,
	"statusDate" as status_date,
	"statusReason" as status_reason,
	"expressedBy" as expressed_by,
	addresses,
	note,
	"outcomeCode" as outcome_code,
	"outcomeReference" as outcome_reference
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		identifier jsonb,
		status text,
		category jsonb,
		priority jsonb,
		description jsonb,
		subject jsonb,
		"startDate" date,
		"startCodeableConcept" jsonb,
		target jsonb,
		"statusDate" date,
		"statusReason" text,
		"expressedBy" jsonb,
		addresses jsonb,
		note jsonb,
		"outcomeCode" jsonb,
		"outcomeReference" jsonb
	)
where
	entry.type='Goal'
;

create materialized view fhir.goal_addresses as
select
	substr(reference,10)::text as addresses
from
	fhir.goal,
	jsonb_to_recordset(addresses) as t2(reference text)
;

----------------------------
-- 3.0
create materialized view fhir.immunization as
select
	identifier,
	status,
	"notGiven" as not_given,
	"vaccineCode" as vaccine_code,
	substr((patient->>'reference'),10)::text as patient,
	substr((encounter->>'reference'),10)::text as encounter,
	date,
	"primarySource" as primary_source,
	"reportOrigin" as report_origin,
	location,
	manufacturer,
	"lotNumber" as lot_number,
	"expirationDate" as expiration_date,
	site,
	route,
	"doseQuantity" as dose_quantity,
	practitioner,
	note,
	explanation,
	reaction,
	"vaccinationProtocol" as vaccination_protocol
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		identifier jsonb,
		status text,
		"notGiven" boolean,
		"vaccineCode" jsonb,
		patient jsonb,
		encounter jsonb,
		date timestamp,
		"primarySource" boolean,
		"reportOrigin" jsonb,
		location jsonb,
		manufacturer jsonb,
		"lotNumber" text,
		"expirationDate" date,
		site jsonb,
		route jsonb,
		"doseQuantity" jsonb,
		practitioner jsonb,
		note jsonb,
		explanation jsonb,
		reaction jsonb,
		"vaccinationProtocol" jsonb
	)
where
	entry.type='Immunization'
;

create materialized view fhir.immunization_code as
select
	patient,
	encounter,
	text,
	t2.*
from
	fhir.immunization,
	jsonb_to_record(vaccine_code) as t(text text, coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

----------------------------
-- 3.0
create materialized view fhir.medication as
select
	identifier,
	code,
	status,
	"isBrand" as is_brand,
	"isOverTheCounter" as is_over_the_counter,
	manufacturer,
	form,
	ingredient,
	package,
	image
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		identifier jsonb,
		code jsonb,
		status text,
		"isBrand" boolean,
		"isOverTheCounter" boolean,
		manufacturer jsonb,
		form jsonb,
		ingredient jsonb,
		package jsonb,
		image jsonb
	)
where
	entry.type='Medication'
;

create materialized view fhir.medication_code as
select
	text,
	t2.*
from
	fhir.medication,
	jsonb_to_record(code) as t(text text, coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

----------------------------
-- 3.0
create materialized view fhir.medication_request as
select
	identifier,
	definition,
	"basedOn" as based_on,
	"groupIdentifier" as group_identifier,
	status,
	intent,
	category,
	priority,
	"medicationCodeableConcept" as medication_codeable_concept,
	substr(("medicationReference"->>'reference'),10)::text as medication_reference,
	substr((subject->>'reference'),10)::text as subject,
	substr((context->>'reference'),10)::text as context,
	"supportingInformation" as supporting_information,
	"authoredOn" as authored_on,
	requester,
	recorder,
	"reasonCode" as reason_code,
	substr(("reasonReference"->>'reference'),10)::text as reason_reference,
	note,
	"dosageInstruction" as dosage_instruction,
	"dispenseRequest" as dispense_request,
	substitution,
	"priorPrescription" as prior_prescription,
	"detectedIssue" as detected_issue,
	"eventHistory" as event_history,
	extension -- split out
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		identifier jsonb,
		definition jsonb,
		"basedOn" jsonb,
		"groupIdentifier" jsonb,
		status text,
		intent text,
		category jsonb,
		priority text,
		"medicationCodeableConcept" jsonb,
		"medicationReference" jsonb,
		subject jsonb,
		context jsonb,
		"supportingInformation" jsonb,
		"authoredOn" timestamp,
		requester jsonb,
		recorder jsonb,
		"reasonCode" jsonb,
		"reasonReference" jsonb,
		note jsonb,
		"dosageInstruction" jsonb,
		"dispenseRequest" jsonb,
		substitution jsonb,
		"priorPrescription" jsonb,
		"detectedIssue" jsonb,
		"eventHistory" jsonb,
		extension jsonb -- part of Synthea
	)
where
	entry.type='MedicationRequest'
;

----------------------------
-- 3.0
create materialized view fhir.observation as
select
	id,
	identifier,
	"basedOn" as based_on,
	status,
	category,
	code,
	substr((subject->>'reference'),10)::text as subject,
	substr((context->>'reference'),10)::text as context,
	"effectiveDateTime" as effective_date_time,
	"effectivePeriod" as effective_period,
	issued
	performer,
	"valueQuantity"->>'code' as value_quantity_code,
	"valueQuantity"->>'unit' as value_quantity_unit,
	"valueQuantity"->>'value' as value_quantity_value,
	"valueQuantity"->>'system' as value_quantity_system,
	"valueCodeableConcept" as value_codeable_concept,
	"valueString" as value_string,
	"valueBoolean" as value_boolean,
	"valueRange" as value_range,
	"valueRatio" as value_ratio,
	"valueSampledData" as value_sample_data,
	"valueTime" as value_time,
	"valueDateTime" as value_date_time,
	"valuePeriod" as value_period,
	"valueAttachment" as value_attachment,
	"valueReference" as value_reference,
	"dataAbsentReason" as data_absent_reason,
	interpretation,
	comment,
	"bodySite" as body_site,
	method,
	specimen,
	device,
	"referenceRange" as reference_range,
	related,
	component
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		id text, -- present in Synthea
		identifier jsonb,
		"basedOn" jsonb,
		status text,
		category jsonb,
		code jsonb,
		subject jsonb,
		context jsonb,
		"effectiveDateTime" timestamp,
		"effectivePeriod" jsonb,
		issued timestamp,
		performer jsonb,
		"valueQuantity" jsonb,
		"valueCodeableConcept" jsonb,
		"valueString" text,
		"valueBoolean" boolean,
		"valueRange" jsonb,
		"valueRatio" jsonb,
		"valueSampledData" jsonb,
		"valueTime" time,
		"valueDateTime" timestamp,
		"valuePeriod" jsonb,
		"valueAttachment" jsonb,
		"valueReference" jsonb,
		"dataAbsentReason" jsonb,
		interpretation jsonb,
		comment text,
		"bodySite" jsonb,
		method jsonb,
		specimen jsonb,
		device jsonb,
		"referenceRange" jsonb,
		related jsonb,
		component jsonb
	)
where
	entry.type='Observation'
;

create materialized view fhir.observation_code as
select
	id,
	text,
	t2.*
from
	fhir.observation,
	jsonb_to_record(code) as t(text text, coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

create materialized view fhir.observation_category as
select
	id,
	t2.*
from
	fhir.observation,
	jsonb_to_recordset(category) as t(coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text)
;

----------------------------
-- 3.0
create materialized view fhir.organization as
select
	id,
	active,
	t.type,
	name,
	alias,
	telecom,
	address,
	"partOf" as part_of,
	contact,
	endpoint
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		id text, -- present in Synthea
		active boolean,
		type jsonb,
		name text,
		alias jsonb,
		telecom jsonb,
		address jsonb,
		"partOf" jsonb,
		contact jsonb,
		endpoint jsonb
	)
where
	entry.type='Organization'
;

create materialized view fhir.organization_type as
select
	id,
	text,
	t2.*
from
	fhir.organization,
	jsonb_to_recordset(type) as t(text text, coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;

----------------------------
-- 3.0
create materialized view fhir.patient as
select
	id,
	identifier,
	active,
	name,
	telecom,
	gender,
	"birthDate" as birth_date,
	"deceasedBoolean" as deceased_boolean,
	"deceasedDateTime" as deceased_date_time,
	address,
	"maritalStatus"->>'text' as marital_status,
	"multipleBirthBoolean" as multiple_birth_boolean,
	"multipleBirthInteger" as multiple_birth_integer,
	photo,
	contact,
	animal,
	communication,
	"generalPractitioner" as general_practitioner,
	"managingOrganization" as managing_organization,
	link,
	extension
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		id text, -- part of Synthea
		identifier jsonb,
		active boolean,
		name jsonb,
		telecom jsonb,
		gender text,
		"birthDate" date,
		"deceasedBoolean" boolean,
		"deceasedDateTime" timestamp,
		address jsonb,
		"maritalStatus" jsonb,
		"multipleBirthBoolean" boolean,
		"multipleBirthInteger" int,
		photo jsonb,
		contact jsonb,
		animal jsonb,
		communication jsonb,
		"generalPractitioner" jsonb,
		"managingOrganization" jsonb,
		link jsonb,
		extension jsonb -- part of Synthea
	)
where
	entry.type='Patient'
;

create materialized view fhir.patient_identifier as
select
	id,
	pi.*
from
	fhir.patient,
	jsonb_to_recordset(identifier) as pi(type jsonb, value text, system text)
;

create materialized view fhir.patient_telecom as
select
	id,
	t.*
from
	fhir.patient,
	jsonb_to_recordset(telecom) as t(use text, value text, system text)
;

create materialized view fhir.patient_extension_race as
select
	id,
	url,
	"valueCodeableConcept"->>'text' as text,
	t2.*
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueCodeableConcept" jsonb),
	jsonb_to_recordset(("valueCodeableConcept"->>'coding')::jsonb) as t2(code text, system text, display text)
where
	url='http://hl7.org/fhir/us/core/StructureDefinition/us-core-race'
;

create materialized view fhir.patient_extension_ethnicity as
select
	id,
	url,
	"valueCodeableConcept"->>'text' as text,
	t2.*
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueCodeableConcept" jsonb),
	jsonb_to_recordset(("valueCodeableConcept"->>'coding')::jsonb) as t2(code text, system text, display text)
where
	url='http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity'
;

create materialized view fhir.patient_extension_birthplace as
select
	id,
	url,
	"valueAddress"->>'city' as city,
	"valueAddress"->>'state' as state,
	"valueAddress"->>'country' as country
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueAddress" jsonb)
where
	url='http://hl7.org/fhir/StructureDefinition/birthPlace'
;

create materialized view fhir.patient_extension_mother_maiden_name as
select
	id,
	url,
	"valueString" as mother_maiden_name
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueString" text)
where
	url='http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName'
;

create materialized view fhir.patient_extension_birth_sex as
select
	id,
	url,
	"valueCode" as birth_sex
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueCode" text)
where
	url='http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex'
;

create materialized view fhir.patient_extension_interpreter_required as
select
	id,
	url,
	"valueBoolean" as interpreter_required
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueBoolean" jsonb)
where
	url='http://hl7.org/fhir/StructureDefinition/patient-interpreterRequired'
;

create materialized view fhir.patient_extension_fictional_person as
select
	id,
	url,
	"valueBoolean" as interpreter_required
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueBoolean" jsonb)
where
	url='http://standardhealthrecord.org/fhir/StructureDefinition/shr-actor-FictionalPerson-extension'
;

create materialized view fhir.patient_extension_father_name as
select
	id,
	url,
	("valueHumanName"->>'text')::text as father_name
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueHumanName" jsonb)
where
	url='http://standardhealthrecord.org/fhir/StructureDefinition/shr-demographics-FathersName-extension'
;

create materialized view fhir.patient_extension_ssn as
select
	id,
	url,
	"valueString"::text as mother_maiden_name
from
	fhir.patient,
	jsonb_to_recordset(extension) as t(url text, "valueString" text)
where
	url='http://standardhealthrecord.org/fhir/StructureDefinition/shr-demographics-SocialSecurityNumber-extension'
;

create materialized view fhir.patient_communication as
select
	id,
	preferred,
	t3.*
from
	fhir.patient,
	jsonb_to_recordset(communication) as t(language jsonb, preferred boolean),
	jsonb_to_record(language) as t2(coding jsonb),
	jsonb_to_recordSet(coding) as t3(code text, system text, display text)
;

create materialized view fhir.patient_address as
select
	id,
	city,
	line,
	state,
	country,
	t.extension,
	"postalCode" as postal_code
from
	fhir.patient,
	jsonb_to_recordset(address) as t(
		city text,
		line jsonb,
		state text,
		country text,
		extension jsonb,
		"postalCode" text
	)
;

create materialized view fhir.patient_name as
select
	id,
	use,
	n.text,
	family,
	jsonb_array_elements_text(given) as given,
	jsonb_array_elements_text(prefix) as prefix,
	jsonb_array_elements_text(suffix) as suffix,
	period
from
	fhir.patient,
	jsonb_to_recordset(name) as n(
		use text,
		text text,
		family text,
		given jsonb,
		prefix jsonb,
		suffix jsonb,
		period jsonb
	)
;

----------------------------
--3.0

create materialized view fhir.procedure as
select
	identifier,
	definition,
	"basedOn" as based_on,
	"partOf" as part_of,
	status,
	"notDone" as not_done,
	"notDoneReason" as not_done_reason,
	category,
	code,
	substr((subject->>'reference'),10)::text as subject,
	substr((context->>'reference'),10)::text as context,
	"performedDateTime",
	"performedPeriod"->>'start' as performed_period_start,
	"performedPeriod"->>'end' as performed_period_end,
	performer,
	location,
	"reasonCode" as reason_code,
	"reasonReference" as reason_reference, -- split out?
	"bodySite" as body_site,
	outcome,
	report,
	complication,
	"complicationDetail" as complication_detail,
	"followUp" as follow_up,
	note,
	"focalDevice" as focal_device,
	"usedReference" as used_reference,
	"usedCode" as used_code
from
	fhir.entry,
	jsonb_to_record(entry) as t(
		identifier jsonb,
		definition jsonb,
		"basedOn" jsonb,
		"partOf" jsonb,
		status text,
		"notDone" boolean,
		"notDoneReason" jsonb,
		category jsonb,
		code jsonb,
		subject jsonb,
		context jsonb,
		"performedDateTime" jsonb,
		"performedPeriod" jsonb,
		performer jsonb,
		location jsonb,
		"reasonCode" jsonb,
		"reasonReference" jsonb,
		"bodySite" jsonb,
		outcome jsonb,
		report jsonb,
		complication jsonb,
		"complicationDetail" jsonb,
		"followUp" jsonb,
		note jsonb,
		"focalDevice" jsonb,
		"usedReference" jsonb,
		"usedCode" jsonb
	)
where
	entry.type='Procedure'
;

create materialized view fhir.procedure_code as
select
	subject,
	context,
	text,
	t2.*
from
	fhir.procedure,
	jsonb_to_record(code) as t(text text, coding jsonb),
	jsonb_to_recordset(coding) as t2(code text, system text, display text)
;
