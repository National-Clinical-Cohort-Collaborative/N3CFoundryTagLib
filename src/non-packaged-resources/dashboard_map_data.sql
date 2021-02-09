create table enclave_cohort.map_region(id int, name text);

create table enclave_cohort.map_state(id int, name text, abbrev text);

create view enclave_cohort.region_frequency as
select id,name,sum(num_patients) from atients_subregion_for_map,map_region where name=subregion group by 1,2 order by 3;



insert into enclave_cohort.map_region(name,id) values('New England',1);
insert into enclave_cohort.map_region(name,id) values('West South Central',2);
insert into enclave_cohort.map_region(name,id) values('Pacific',3);
insert into enclave_cohort.map_region(name,id) values('Middle Atlantic',4);
insert into enclave_cohort.map_region(name,id) values('East South Central',5);
insert into enclave_cohort.map_region(name,id) values('Mountain',6);
insert into enclave_cohort.map_region(name,id) values('West North Central',7);
insert into enclave_cohort.map_region(name,id) values('East North Central',8);
insert into enclave_cohort.map_region(name,id) values('South Atlantic',9);

insert into enclave_cohort.map_state(name,abbrev,id) values( 'Alabama', 'AL', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Arizona',  'AZ', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Arkansas', 'AR', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'California',  'CA', 3);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Colorado', 'CO', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Connecticut', 'CT', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Delaware',  'DE', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Florida', 'FL', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Georgia',  'GA', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Idaho', 'ID', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Illinois',  'IL', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Indiana', 'IN', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Iowa', 'IA', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Kansas',  'KS', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Kentucky',  'KY', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Louisiana',  'LA', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Maine',  'ME', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Maryland', 'MD', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Massachusetts', 'MA', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Michigan', 'MI', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Minnesota',  'MN', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Mississippi', 'MS', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Missouri', 'MO', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Montana', 'MT', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Nebraska', 'NE', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Nevada', 'NV', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New Hampshire', 'NH', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New Jersey',  'NJ', 4);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New Mexico', 'NM', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'New York', 'NY', 4);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'North Carolina', 'NC', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'North Dakota', 'ND', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Ohio', 'OH', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Oklahoma',  'OK', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Oregon', 'OR', 3);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Pennsylvania', 'PA', 4);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Rhode Island', 'RI', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'South Carolina',  'SC', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'South Dakota',  'SD', 7);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Tennessee',  'TN', 5);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Texas',  'TX', 2);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Utah', 'UT', 6);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Vermont', 'VT', 1);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Virginia', 'VA', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Washington',  'WA', 3);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'West Virginia', 'WV', 9);
insert into enclave_cohort.map_state(name,abbrev,id) values(  'Wisconsin',  'WI', 8);
insert into enclave_cohort.map_state(name,abbrev,id) values( 'Wyoming', 'WY', 6);

