/*---------------------------------------------------------------------------

Name: AN25-10 analysis tables

Purpose: This script creates aggregate analysis tables to support Analytical Note 25/09: Transnationalism in Aotearoa New Zealand: Initial Data Assembly

Developer: Tim Hughes

Review: Sarah Crichton

Dependencies: 	Scripts 1-3 from AN25/09 must be run first.
		- Also depends on some tax summary tables produced separately by Sarah and saved as [IDI_Sandpit].[DL-MAA2013-16].[TaxInc_2000_24_202506_SC]

Output: [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

Subsequent use: Output tables are analysed in R (AN25-10.rmd)

---------------------------------------------------------------------------*/


use idi_clean_202506;

--The following tables need to be updated to reflect the relevant refresh

-- [IDI_Community].[cm_read_HIGHEST_NQFLEVEL_SPELLS].[highest_nqflevel_spells_202506] 
-- [IDI_Sandpit].[DL-MAA2013-16].[TaxInc_2000_24_202506_SC]
-- [IDI_Adhoc].[clean_read_CUS].[migrant_probabilities_202506]

--------------------------------------------------------------------------------------
--Setup temporary tables
--------------------------------------------------------------------------------------

--emigration flag

drop table if exists #emigration

select distinct t.snz_uid
, t.yr
, snz_spine_ind
, case when nz_emigrant = 1 then 1 
		else 0
		end as nz_emigrant
, case when birth_citizen = 1 
			and nz_emigrant = 1 then 1 
		else 0
		end as nz_born_emigrant
, case when snz_cus_journey_uid is not null then 1
		else 0
		end as emigration_flag

into #emigration

from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
left join 
	(select snz_uid
	, year(cus_jou_actual_date) as yr 
	, j1.snz_cus_journey_uid
	, case when cus_jou_citizenship_code = 'NZ' then 1 else 0 end as nz_emigrant
	from cus_clean.journey j1
	left join [IDI_Adhoc].[clean_read_CUS].[migrant_probabilities_202506] p
	on (j1.snz_cus_journey_uid = p.snz_cus_journey_uid)
	where (cus_ltm_final_long_term_mig_ind = 1
			or p.is_long_term_mig = 1
			or p.mean_probability >0.3
			)
	and cus_jou_direction_code = 'D'
	) j
on (t.snz_uid = j.snz_uid and t.yr = j.yr)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
on (t.snz_uid = d.snz_uid)



--Return emigration tables

drop table if exists #emigrant_returns;

select snz_uid
, yr
, min(return_yr-yr) as yrs_till_return

into #emigrant_returns

from
	( select e.snz_uid
	, e.yr
	, t.yr as return_yr
	from
		(select snz_uid
		, yr
		from #emigration 
		where nz_born_emigrant = 1
		) e
	left join 
		(select snz_uid
		, yr
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat]
		where transcat_code = 4
		) t
	on (e.snz_uid = t.snz_uid and e.yr<t.yr)
	) setup

group by snz_uid,yr;



 drop table if exists [IDI_Sandpit].[DL-MAA2013-16].[TimH_percentiles_temp]

  select distinct
  taxyr
  , p01 = PERCENTILE_CONT(0.01) within group (order by market_income) over (partition by taxyr)
  , p10 = PERCENTILE_CONT(0.1) within group (order by market_income) over (partition by taxyr)
  , p20 = PERCENTILE_CONT(0.2) within group (order by market_income) over (partition by taxyr)
  , p25 = PERCENTILE_CONT(0.25) within group (order by market_income) over (partition by taxyr)
  , p30 = PERCENTILE_CONT(0.3) within group (order by market_income) over (partition by taxyr)
  , p40 = PERCENTILE_CONT(0.4) within group (order by market_income) over (partition by taxyr)
  , p50 = PERCENTILE_CONT(0.5) within group (order by market_income) over (partition by taxyr)
  , p60 = PERCENTILE_CONT(0.6) within group (order by market_income) over (partition by taxyr)
  , p70 = PERCENTILE_CONT(0.7) within group (order by market_income) over (partition by taxyr)
  , p75 = PERCENTILE_CONT(0.75) within group (order by market_income) over (partition by taxyr)
  , p80 = PERCENTILE_CONT(0.8) within group (order by market_income) over (partition by taxyr)
  , p90 = PERCENTILE_CONT(0.9) within group (order by market_income) over (partition by taxyr)
  , p99 = PERCENTILE_CONT(0.99) within group (order by market_income) over (partition by taxyr)
   
  into [IDI_Sandpit].[DL-MAA2013-16].[TimH_percentiles_temp]
  from [IDI_Sandpit].[DL-MAA2013-16].[TaxInc_2000_24_202506_SC]

 alter table [IDI_Sandpit].[DL-MAA2013-16].[TimH_percentiles_temp] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

  
drop table if exists [IDI_Sandpit].[DL-MAA2013-16].[TimH_marketinc_percentiles]

select i.*
, case	when market_income <p10 then 1
		when market_income <p20 then 2
		when market_income <p30 then 3
		when market_income <p40 then 4
		when market_income <p50 then 5
		when market_income <p60 then 6
		when market_income <p70 then 7
		when market_income <p80 then 8
		when market_income <p90 then 9
		when market_income >=p90 then 10
		end as marketinc_decile
, case	when market_income <p20 then 1
		when market_income <p40 then 2
		when market_income <p60 then 3
		when market_income <p80 then 4
		when market_income >=p80 then 5
		end as marketinc_quantile
, case	when market_income <p25 then 1
		when market_income <p50 then 2
		when market_income <p75 then 3
		when market_income >=p75 then 4
		end as marketinc_quartile
into [IDI_Sandpit].[DL-MAA2013-16].[TimH_marketinc_percentiles]
from [IDI_Sandpit].[DL-MAA2013-16].[TaxInc_2000_24_202506_SC] i
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_percentiles_temp] p
on (i.taxyr = p.taxyr)

alter table [IDI_Sandpit].[DL-MAA2013-16].[TimH_marketinc_percentiles] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)

drop table if exists [IDI_Sandpit].[DL-MAA2013-16].[TimH_percentiles_temp]

drop table if exists #parent_ids

select d.snz_uid
, case when dia_bir_parent1_sex_snz_code = 1 then parent2_snz_uid
		else parent1_snz_uid
		end as mother_id
, case when dia_bir_parent1_sex_snz_code = 1 then parent1_snz_uid
		else parent2_snz_uid
		end as father_id

into #parent_ids
from [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
left join dia_clean.births b
on (d.snz_uid = b.snz_uid)


--------------------------------------------------------------------------------------
--Create analysis table
--------------------------------------------------------------------------------------


drop table if exists [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

create table [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]
	(
	table_nbr int not null
	, var1 nvarchar(50) null
	, var1_value nvarchar(50) null
	, var2 nvarchar(50) null
	, var2_value nvarchar(50) null
	, var3 nvarchar(50) null
	, var3_value nvarchar(50) null
	, var4 nvarchar(50) null
	, var4_value nvarchar(50) null
	, measure_type1 nvarchar(50) null
	, measure1 nvarchar(50) null
	, measure_type2 nvarchar(50) null
	, measure2 nvarchar(50) null
	)

	-- table 1 - count by transcat, age

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=1;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

	select 1 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, null as var4
	, null as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2
from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	group by t.yr, tl.transcat, b.age, d.male
	) setup



-- table 2- ethnic composition of diaspora vs birth citizens

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=2

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

	select 2 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'ethnic_group' as var4
	, 'NZ European' as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2
from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where t.transcat_code in (4,12)
	and euro = 1
	group by t.yr, tl.transcat, b.age, d.male
	) setup

union

select 2 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'ethnic_group' as var4
	, 'Maori' as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2
from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where maori = 1
	and t.transcat_code in (4,12)
	group by t.yr, tl.transcat, b.age, d.male
	) setup

union

select 2 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'ethnic_group' as var4
	, 'Pacific' as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2
from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where Pacific = 1
	and t.transcat_code in (4,12)
	group by t.yr, tl.transcat, b.age, d.male
	) setup

union

select 2 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'ethnic_group' as var4
	, 'Asian' as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2
from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where asian = 1
	and t.transcat_code in (4,12)
	group by t.yr, tl.transcat, b.age, d.male
	) setup

union

select 2 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'ethnic_group' as var4
	, 'MELAA' as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2
from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where melaa = 1
	and t.transcat_code in (4,12)
	group by t.yr, tl.transcat, b.age, d.male
	) setup

union

select 2 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'ethnic_group' as var4
	, 'Other' as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2

from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where other_eth = 1
	and t.transcat_code in (4,12)
	group by t.yr, tl.transcat, b.age, d.male
	) setup

union

select 2 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'ethnic_group' as var4
	, 'Unknown' as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2

from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where Unk_eth = 1
	and t.transcat_code in (4,12)
	group by t.yr, tl.transcat, b.age, d.male
	) setup

-- table 3- gender composition of diaspora vs birth citizens

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=3

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 3 as table_nbr
	, 'yr' as var1
	, convert(nvarchar,setup.yr) as var1_value
	, 'transcat' as var2
	, convert(nvarchar,setup.transcat) as var2_value
	, 'age' as var3
	, convert(nvarchar,setup.age) as var3_value
	, 'Male' as var4
	, convert(nvarchar,setup.male) as var4_value
	, 'count' as measure_type1
	, setup.num as measure1
	, null as measure_type2
	, null as measure2

from
	(
	select t.yr
	,tl.transcat
	, b.age
	, d.male
	, 'count' as measure_type1
	, count(*) as num
		from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
		on (t.transcat_code = tl.transcat_code)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (t.snz_uid = b.snz_uid and t.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (t.snz_uid = d.snz_uid)
	where t.transcat_code in (4,12)
	group by t.yr, tl.transcat, b.age, d.male
	) setup

-- table 4 - count by transcat and age for NZ-born citizens

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=4

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 4 as table_nbr
, 'yr' as var1
, convert(nvarchar,t.yr) as var1_value
, 'transcat' as var2
, convert(nvarchar,transcat) as var2_value
, 'birth_yr' as var3
, convert(nvarchar,birth_yr) as var3_value
, null as var4
, null as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	where birth_citizen = 1
group by t.yr, transcat, birth_yr


-- table 5 - count of emigration flows under different definitions

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=5

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 5 as table_nbr
, 'yr' as var1
, convert(nvarchar,t.yr) as var1_value
, 'birth_citizen' as var2
, convert(nvarchar,birth_citizen) as var2_value
, 'spine_ind' as var3
, convert(nvarchar,d.snz_spine_ind) as var3_value
, 'emigration_measure' as var4
, 'people' as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
where nz_emigrant = 1

group by t.yr,birth_citizen, d.snz_spine_ind

union

select 5 as table_nbr
, 'yr' as var1
, convert(nvarchar,year(cus_jou_actual_date)) as var1_value
, 'birth_citizen' as var2
, convert(nvarchar,birth_citizen) as var2_value
, 'spine_ind' as var3
, convert(nvarchar,d.snz_spine_ind) as var3_value
, 'emigration_measure' as var4
, 'departures' as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2

from cus_clean.journey j
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
on (j.snz_uid = b.snz_uid and year(cus_jou_actual_date) = b.yr)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
on (j.snz_uid = d.snz_uid)

where cus_ltm_final_long_term_mig_ind = 1
and cus_jou_direction_code = 'D'
and cus_jou_citizenship_code = 'NZ'
	
group by year(cus_jou_actual_date),birth_citizen, d.snz_spine_ind

union

select 5 as table_nbr
, 'yr' as var1
, convert(nvarchar,t.yr-1) as var1_value
, 'birth_citizen' as var2
, convert(nvarchar,birth_citizen) as var2_value
, 'spine_ind' as var3
, convert(nvarchar,d.snz_spine_ind) as var3_value
, 'emigration_measure' as var4
, 'transcat change' as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2

from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
on (t.snz_uid = b.snz_uid and t.yr = b.yr)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
on (t.snz_uid = d.snz_uid)

where transcat_code = 12
and years_in_transcat = 2
	
group by t.yr-1,birth_citizen, d.snz_spine_ind

-- table 51 - count of emigration flows under different definitions

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=51

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 51 as table_nbr
, 'yr' as var1
, convert(nvarchar,t.yr) as var1_value
, 'age_10bins' as var2
, convert(nvarchar,10*floor(age/10)) as var2_value
, 'emigrant' as var3
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var3_value
, null as var4
, null as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
where (nz_born_emigrant = 1 or t.transcat_code = 4)

group by t.yr
		, 10*floor(age/10)
		,case	when nz_born_emigrant = 1 then 'emigrant'
				when t.transcat_code = 4 then 'resident'
				else null 
				end



-- table 6 - count of emigration flows by year, age and highest qualification

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=6

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 6 as table_nbr
, 'yr' as var1
, convert(nvarchar,t.yr) as var1_value
, 'age_5bin' as var2
, convert(nvarchar,5*floor(age/5)) as var2_value
, 'qual' as var3
, convert(nvarchar,max_nqflevel_sofar) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where (nz_born_emigrant = 1 or t.transcat_code = 4)
group by 5*floor(age/5)
			, max_nqflevel_sofar
			, t.yr
			, case	when nz_born_emigrant = 1 then 'emigrant'
					when t.transcat_code = 4 then 'resident'
					else null
					end


-- table 7- emigration rates by qualification by gender for Europeans aged 20-29

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=7

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 7 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'gender' as var2
, case when male = 1 then 'Male' when male = 0 then 'Female' else 'Unknown' end as var2_value
, 'qual' as var3
, convert(nvarchar,max_nqflevel_sofar) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where 10*floor(age/10) = 20
and Euro = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by male, max_nqflevel_sofar, t.yr, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end


-- table 8- emigration rates by qualification by gender for Asians aged 20-29

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=8

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 8 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'gender' as var2
, case when male = 1 then 'Male' when male = 0 then 'Female' else 'Unknown' end as var2_value
, 'qual' as var3
, convert(nvarchar,max_nqflevel_sofar) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where 10*floor(age/10) = 20
and Asian = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)
	

group by male
		, max_nqflevel_sofar
		, t.yr
		, case	when nz_born_emigrant = 1 then 'emigrant'
				when t.transcat_code = 4 then 'resident'
				else null
				end

-- table 9- emigration rates by qualification by gender for Maori people aged 20-29

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=9

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 9 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'gender' as var2
, case when male = 1 then 'Male' when male = 0 then 'Female' else 'Unknown' end as var2_value
, 'qual' as var3
, convert(nvarchar,max_nqflevel_sofar) as var3_value
, 'status' as var4
, case	when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where 10*floor(age/10) = 20
and Maori = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by male
		, max_nqflevel_sofar
		, t.yr
		, case	when nz_born_emigrant = 1 then 'emigrant'
				when t.transcat_code = 4 then 'resident'
				else null
				end


-- table 10- emigration rates by qualification by gender for Pacific people aged 20-29

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=10

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 10 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'gender' as var2
, case when male = 1 then 'Male' when male = 0 then 'Female' else 'Unknown' end as var2_value
, 'qual' as var3
, convert(nvarchar,max_nqflevel_sofar) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where 10*floor(age/10) = 20
and Pacific = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by male
		, max_nqflevel_sofar
		, t.yr
		, case	when nz_born_emigrant = 1 then 'emigrant'
				when t.transcat_code = 4 then 'resident'
				else null
				end

-- table 101 - Emigration rates of children by ethnicity

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=101

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 101 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'ethnicity' as var2
, 'Euro' as var2_value
, 'age' as var3
, convert(nvarchar,5*floor(age/5)) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where age < 20
and Euro = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by 5*floor(age/5), t.yr, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end


insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 101 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'ethnicity' as var2
, 'Asian' as var2_value
, 'age' as var3
, convert(nvarchar,5*floor(age/5)) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where age < 20
and Asian = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by 5*floor(age/5), t.yr, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 101 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'ethnicity' as var2
, 'Maori' as var2_value
, 'age' as var3
, convert(nvarchar,5*floor(age/5)) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where age < 20
and Maori = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by 5*floor(age/5), t.yr, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 101 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'ethnicity' as var2
, 'Pacific' as var2_value
, 'age' as var3
, convert(nvarchar,5*floor(age/5)) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (t.snz_uid = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where age < 20
and Pacific = 1
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by 5*floor(age/5), t.yr, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end


-- table 11 - Emigration rates of children by parental income


delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=11

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 11 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'age' as var2
, convert(nvarchar,5*floor(age/5)) as var2_value
, 'mother_inc_percentile' as var3
, convert(nvarchar,marketinc_quartile) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join #parent_ids p
	on  (t.snz_uid = p.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_marketinc_percentiles] i
	on (p.mother_id = i.snz_uid and i.taxyr = (t.yr-2))

where age <20
and	(nz_born_emigrant = 1
or (t.transcat_code = 4))

group by 5*floor(age/5), marketinc_quartile, t.yr,  case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end 


delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=12

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 12 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'age' as var2
, convert(nvarchar,5*floor(age/5)) as var2_value
, 'father_inc_percentile' as var3
, convert(nvarchar,marketinc_quartile) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join #parent_ids p
	on  (t.snz_uid = p.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_marketinc_percentiles] i
	on (p.father_id = i.snz_uid and i.taxyr = (t.yr-2))

where age <20
and	(nz_born_emigrant = 1
or (t.transcat_code = 4))

group by 5*floor(age/5), marketinc_quartile, t.yr,  case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end 




-- table 13- emigration rates by maternal qualification for children

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=13

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 13 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'age' as var2
, convert(nvarchar,5*floor(age/5)) var2_value
, 'maternal qual' as var3
, convert(nvarchar,max_nqflevel_sofar) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join #parent_ids p
	on  (t.snz_uid = p.snz_uid)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (p.mother_id = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where age<20
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by 5*floor(age/5), max_nqflevel_sofar, t.yr, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end


-- table 14 - emigration rates by paternal qualification for children

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=14

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 14 as table_nbr
, 'yr' as var1
, t.yr as var1_value
, 'age' as var2
, convert(nvarchar,5*floor(age/5)) var2_value
, 'paternal qual' as var3
, convert(nvarchar,max_nqflevel_sofar) as var3_value
, 'status' as var4
, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)
	left join #emigration e
	on (t.snz_uid = e.snz_uid and t.yr = e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (t.snz_uid = d.snz_uid)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join #parent_ids p
	on  (t.snz_uid = p.snz_uid)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (p.father_id = q.snz_uid and t.yr >= year(q.nqf_attained_date) and t.yr <= year(q.until_date))
where age<20
and (nz_born_emigrant = 1 or t.transcat_code = 4)

group by 5*floor(age/5), max_nqflevel_sofar, t.yr, case when nz_born_emigrant = 1 then 'emigrant'
		when t.transcat_code = 4 then 'resident'
		else null
		end


--table 16 overall return rates by year

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=16;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 16 as table_nbr
, 'yr' as var1
, convert(nvarchar,setup.yr) as var1_value
, 'yrs_till_return' as var2
, convert(nvarchar,setup.yrs_till_return) as var2_value
, null as var3
, null as var3_value
, null as var4
, null as var4_value
, 'cumulative_returns' as measure_type1
, cumulative_returns as measure1
, 'emigrants' as measure_type2
, emigrants as measure2

from
	(
	select
	s1.yr
	, yrs_till_return
	, sum(returners) over (partition by s1.yr order by yrs_till_return) as cumulative_returns
	, min(emigrants) over (partition by s1.yr) as emigrants
	from
		(
		select yr
		, yrs_till_return
		, count(*) as returners
		from #emigrant_returns
		where yrs_till_return is not null
		group by yr, yrs_till_return
		) s1
	left join
		(select yr, count(*) as emigrants
		from #emigrant_returns
		group by yr
		) s2
	on (s1.yr=s2.yr)
	) setup

--table 160 diasporic status of emigrees by years since emigration

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=160;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 160 as table_nbr
, 'emigration_year' as var1
, convert(nvarchar,e.yr) as var1_value
, 'yrs_since_emigration' as var2
, convert(nvarchar,t.yr-e.yr) as var2_value
, 'transcat' as var3
, transcat as var3_value
, null as var4
, null as var4_value
, 'count' as measure_type1
, count(*) as measure1
, 'emigrant_cohort'as measure_type2
, min(emigrant_cohort) as measure2

from #emigration e
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
on (e.snz_uid=t.snz_uid and t.yr>e.yr)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
on (t.transcat_code = tl.transcat_code)
left join 
	(select yr, count(*) as emigrant_cohort
	from #emigration 
	where nz_born_emigrant=1
	group by yr
	) e2
on e.yr=e2.yr

where nz_born_emigrant = 1
and t.transcat_code in (4,12)

group by e.yr, t.yr-e.yr,transcat


--table 17 return migration rates by age and year

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=17;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 17 as table_nbr
, 'yr' as var1
, convert(nvarchar,setup.yr) as var1_value
, 'yrs_till_return' as var2
, convert(nvarchar,setup.yrs_till_return) as var2_value
, 'age_10bins' as var3
, convert(nvarchar,age) as var3_value
, null as var4
, null as var4_value
, 'cumulative_returns' as measure_type1
, cumulative_returns as measure1
, 'emigrants' as measure_type2
, emigrants as measure2

from
	(
	select
	s1.yr
	, yrs_till_return
	, s1.age
	, sum(returners) over (partition by s1.age, s1.yr order by yrs_till_return) as cumulative_returns
	, min(emigrants) over (partition by s1.age, s1.yr) as emigrants
	from
		(
		select e.yr
		, 10*floor(age/10) as age
		, yrs_till_return
		, count(*) as returners
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		where yrs_till_return is not null
		group by e.yr, 10*floor(age/10), yrs_till_return
		) s1
	left join
		(select e.yr
		, 10*floor(age/10) as age
		, count(*) as emigrants
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		group by e.yr, 10*floor(age/10)
		) s2
	on (s1.yr=s2.yr and s1.age=s2.age)
	) setup

--table 170 diasporic status of emigrees by age and years since emigration

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=170;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 170 as table_nbr
, 'emigration_year' as var1
, convert(nvarchar,e.yr) as var1_value
, 'yrs_since_emigration' as var2
, convert(nvarchar,t.yr-e.yr) as var2_value
, 'transcat' as var3
, transcat as var3_value
, 'age10bins' as var4
, 10*floor(b.age/10) as var4_value
, 'count' as measure_type1
, count(*) as measure1
, 'emigrant_cohort'as measure_type2
, min(emigrant_cohort) as measure2

from #emigration e
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
on (e.snz_uid=t.snz_uid and t.yr>e.yr)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
on (e.snz_uid=b.snz_uid and e.yr=b.yr)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
on (t.transcat_code = tl.transcat_code)
left join 
	(select e.yr, 10*floor(age/10) as age10bins,count(*) as emigrant_cohort
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid = b.snz_uid and e.yr=b.yr)
	where nz_born_emigrant=1
	and age is not null
	and age <80
	group by e.yr, 10*floor(age/10)
	) e2
on (b.yr=e2.yr and e2.age10bins=10*floor(b.age/10))

where nz_born_emigrant = 1
and t.transcat_code in (4,12)
and b.age is not null
and b.age <80

group by e.yr, t.yr-e.yr,transcat, 10*floor(b.age/10)




--table 18 - return rates by qualification for those aged 20-39 emigrating in 2013

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=18;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 18 as table_nbr
, 'yr' as var1
, convert(nvarchar,setup.yr) as var1_value
, 'yrs_till_return' as var2
, convert(nvarchar,setup.yrs_till_return) as var2_value
, 'age_10bins' as var3
, convert(nvarchar,age) as var3_value
, 'qual' as var4
, convert(nvarchar,qual) as var4_value
, 'cumulative_returns' as measure_type1
, cumulative_returns as measure1
, 'emigrants' as measure_type2
, emigrants as measure2

from
	(
	select
	s1.yr
	, yrs_till_return
	, s1.age
	, s1.qual
	, sum(returners) over (partition by s1.age, s1.yr, s1.qual order by yrs_till_return) as cumulative_returns
	, min(emigrants) over (partition by s1.age, s1.yr, s1.qual) as emigrants
	from
		(
		select e.yr
		, 10*floor(age/10) as age
		, case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end as qual
		, yrs_till_return
		, count(*) as returners
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
		on (e.snz_uid = q.snz_uid and e.yr >= year(q.nqf_attained_date) and e.yr <= year(q.until_date))
		where yrs_till_return is not null
		group by e.yr, 10*floor(age/10), yrs_till_return, case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end
		) s1
	left join
		(select e.yr
		, 10*floor(age/10) as age
		, case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end as qual
		, count(*) as emigrants
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
		on (e.snz_uid = q.snz_uid and e.yr >= year(q.nqf_attained_date) and e.yr <= year(q.until_date))
		group by e.yr, 10*floor(age/10), case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end
		) s2
	on (s1.yr=s2.yr and s1.age=s2.age and s1.qual = s2.qual)
	where s1.yr=2013
	and 20*floor(s1.age/20)=20
	) setup

--table 180 diasporic status of emigrees by qual and years since emigration for those aged 20-29 emigrating in 2013

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=180;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 180 as table_nbr
, 'age10bins' as var1
, convert(nvarchar,s1.age10bins) as var1_value
, 'yrs_since_emigration' as var2
, convert(nvarchar,s1.years_since_emigration) as var2_value
, 'transcat' as var3
, s1.transcat as var3_value
, 'qual' as var4
,  s1.qual as var4_value
, 'count' as measure_type1
, convert(nvarchar,transcat_count) as measure1
, 'emigrant_cohort'as measure_type2
, convert(nvarchar,emigrant_cohort) as measure2

from 

	(select e.yr as emigration_yr
	, t.yr-e.yr as years_since_emigration
	, case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end as qual
	, 10*floor(age/10) as age10bins
	, transcat
	, count(*) as transcat_count
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	on (e.snz_uid=t.snz_uid and t.yr>e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr>b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
	on (t.transcat_code = tl.transcat_code)  
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (e.snz_uid = q.snz_uid and e.yr >= year(q.nqf_attained_date) and e.yr <= year(q.until_date))
	
	where nz_born_emigrant = 1
	and t.transcat_code in (4,12)
	and 20*floor(age/20)=20
	group by e.yr
	, t.yr-e.yr
	, transcat
	, 10*floor(age/10)
	, case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end
	) s1
left join
	(select e.yr as emigration_yr
	, 10*floor(age/10) as age10bins
	, case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end as qual
				,count(*) as emigrant_cohort
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr>b.yr)
	left join [IDI_Community].[edu_highest_nqflevel_spells].[highest_nqflevel_spells_202506] q
	on (e.snz_uid = q.snz_uid and e.yr >= year(q.nqf_attained_date) and e.yr <= year(q.until_date))
	where nz_born_emigrant=1
	and 20*floor(age/20)=20
	group by e.yr
	, 10*floor(age/10)
	, case	when max_nqflevel_sofar is null then 'None'
				when max_nqflevel_sofar =0 then 'None'
				when max_nqflevel_sofar <3 then 'School'
				when max_nqflevel_sofar <7 then '4-6 Tertiary'
				when max_nqflevel_sofar >=7 then '7+ Tertiary'
				else 'Unknown'
				end
	) s2
on (s1.emigration_yr=s2.emigration_yr and s1.qual = s2.qual and s1.age10bins=s2.age10bins)

where s1.emigration_yr = 2013


--table 19 - return rates by ethnicity

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=19;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 19 as table_nbr
, 'yr' as var1
, convert(nvarchar,setup.yr) as var1_value
, 'yrs_till_return' as var2
, convert(nvarchar,setup.yrs_till_return) as var2_value
, 'age_10bins' as var3
, convert(nvarchar,age) as var3_value
, 'ethnicity' as var4
, 'Euro' as var4_value
, 'cumulative_returns' as measure_type1
, cumulative_returns as measure1
, 'emigrants' as measure_type2
, emigrants as measure2

from
	(
	select
	s1.yr
	, yrs_till_return
	, s1.age
	, sum(returners) over (partition by s1.age, s1.yr order by yrs_till_return) as cumulative_returns
	, min(emigrants) over (partition by s1.age, s1.yr) as emigrants
	from
		(
		select e.yr
		, 10*floor(age/10) as age
		, yrs_till_return
		, count(*) as returners
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid)
		where yrs_till_return is not null
		and euro=1
		group by e.yr, 10*floor(age/10), yrs_till_return
		) s1
	left join
		(select e.yr
		, 10*floor(age/10) as age
		, count(*) as emigrants
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid) 
		where euro=1
		group by e.yr, 10*floor(age/10)
		) s2
	on (s1.yr=s2.yr and s1.age=s2.age)

	) setup

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 19 as table_nbr
, 'yr' as var1
, convert(nvarchar,setup.yr) as var1_value
, 'yrs_till_return' as var2
, convert(nvarchar,setup.yrs_till_return) as var2_value
, 'age_10bins' as var3
, convert(nvarchar,age) as var3_value
, 'ethnicity' as var4
, 'Asian' as var4_value
, 'cumulative_returns' as measure_type1
, cumulative_returns as measure1
, 'emigrants' as measure_type2
, emigrants as measure2

from
	(
	select
	s1.yr
	, yrs_till_return
	, s1.age
	, sum(returners) over (partition by s1.age, s1.yr order by yrs_till_return) as cumulative_returns
	, min(emigrants) over (partition by s1.age, s1.yr) as emigrants
	from
		(
		select e.yr
		, 10*floor(age/10) as age
		, yrs_till_return
		, count(*) as returners
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid)
		where yrs_till_return is not null
		and Asian=1
		group by e.yr, 10*floor(age/10), yrs_till_return
		) s1
	left join
		(select e.yr
		, 10*floor(age/10) as age
		, count(*) as emigrants
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid)
		where Asian=1
		group by e.yr, 10*floor(age/10)
		) s2
	on (s1.yr=s2.yr and s1.age=s2.age)

	) setup

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 19 as table_nbr
, 'yr' as var1
, convert(nvarchar,setup.yr) as var1_value
, 'yrs_till_return' as var2
, convert(nvarchar,setup.yrs_till_return) as var2_value
, 'age_10bins' as var3
, convert(nvarchar,age) as var3_value
, 'ethnicity' as var4
, 'Maori' as var4_value
, 'cumulative_returns' as measure_type1
, cumulative_returns as measure1
, 'emigrants' as measure_type2
, emigrants as measure2

from
	(
	select
	s1.yr
	, yrs_till_return
	, s1.age
	, sum(returners) over (partition by s1.age, s1.yr order by yrs_till_return) as cumulative_returns
	, min(emigrants) over (partition by s1.age, s1.yr) as emigrants
	from
		(
		select e.yr
		, 10*floor(age/10) as age
		, yrs_till_return
		, count(*) as returners
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid)
		where yrs_till_return is not null
		and Maori=1
		group by e.yr, 10*floor(age/10), yrs_till_return
		) s1
	left join
		(select e.yr
		, 10*floor(age/10) as age
		, count(*) as emigrants
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid)
		where Maori=1
		group by e.yr, 10*floor(age/10)
		) s2
	on (s1.yr=s2.yr and s1.age=s2.age)

	) setup

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 19 as table_nbr
, 'yr' as var1
, convert(nvarchar,setup.yr) as var1_value
, 'yrs_till_return' as var2
, convert(nvarchar,setup.yrs_till_return) as var2_value
, 'age_10bins' as var3
, convert(nvarchar,age) as var3_value
, 'ethnicity' as var4
, 'Pacific' as var4_value
, 'cumulative_returns' as measure_type1
, cumulative_returns as measure1
, 'emigrants' as measure_type2
, emigrants as measure2

from
	(
	select
	s1.yr
	, yrs_till_return
	, s1.age
	, sum(returners) over (partition by s1.age, s1.yr order by yrs_till_return) as cumulative_returns
	, min(emigrants) over (partition by s1.age, s1.yr) as emigrants
	from
		(
		select e.yr
		, 10*floor(age/10) as age
		, yrs_till_return
		, count(*) as returners
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid)
		where yrs_till_return is not null
		and Pacific=1
		group by e.yr, 10*floor(age/10), yrs_till_return
		) s1
	left join
		(select e.yr
		, 10*floor(age/10) as age
		, count(*) as emigrants
		from #emigrant_returns e
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
		on (e.snz_uid = b.snz_uid and e.yr=b.yr)
		left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
		on (e.snz_uid = d.snz_uid)
		where Pacific=1
		group by e.yr, 10*floor(age/10)
		) s2
	on (s1.yr=s2.yr and s1.age=s2.age)

	) setup

--table 190 - diasporic status of emigrees by age, ethnicity and years since emigration

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=190;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 190 as table_nbr
, 'emigration_year' as var1
, convert(nvarchar,s1.emigration_yr) as var1_value
, 'yrs_since_emigration' as var2
, convert(nvarchar,years_since_emigration) as var2_value
, 'ethnicity' as var3
, 'NZ European' as var3_value
, 'age20bins' as var4
, convert(nvarchar,s1.age20bins) as var4_value
, 'resident' as measure_type1
, transcat_count as measure1
, 'emigrant_cohort' as measure_type2
, emigrant_cohort as measure2

from 
(select e.yr as emigration_yr
	, t.yr-e.yr as years_since_emigration
	, 20*floor(age/20) as age20bins
	, count(*) as transcat_count
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	on (e.snz_uid=t.snz_uid and t.yr>e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	
	where nz_born_emigrant = 1
	and t.transcat_code = 4
	and euro=1
	group by e.yr, t.yr-e.yr, 20*floor(age/20)
	) s1
left join
	(select e.yr as emigration_yr
	, 20*floor(age/20) as age20bins
	,count(*) as emigrant_cohort
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	where nz_born_emigrant=1
	and euro=1
	group by e.yr
	, 20*floor(age/20)
	) s2
on (s1.emigration_yr=s2.emigration_yr and s1.age20bins=s2.age20bins)


insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 190 as table_nbr
, 'emigration_year' as var1
, convert(nvarchar,s1.emigration_yr) as var1_value
, 'yrs_since_emigration' as var2
, convert(nvarchar,years_since_emigration) as var2_value
, 'ethnicity' as var3
, 'Asian' as var3_value
, 'age20bins' as var4
, convert(nvarchar,s1.age20bins) as var4_value
, 'resident' as measure_type1
, transcat_count as measure1
, 'emigrant_cohort' as measure_type2
, emigrant_cohort as measure2

from 
(select e.yr as emigration_yr
	, t.yr-e.yr as years_since_emigration
	, 20*floor(age/20) as age20bins
	, count(*) as transcat_count
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	on (e.snz_uid=t.snz_uid and t.yr>e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	
	where nz_born_emigrant = 1
	and t.transcat_code = 4
	and Asian=1
	group by e.yr, t.yr-e.yr, 20*floor(age/20)
	) s1
left join
	(select e.yr as emigration_yr
	, 20*floor(age/20) as age20bins
	,count(*) as emigrant_cohort
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	where nz_born_emigrant=1
	and Asian=1
	group by e.yr
	, 20*floor(age/20)
	) s2
on (s1.emigration_yr=s2.emigration_yr and s1.age20bins=s2.age20bins)

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 190 as table_nbr
, 'emigration_year' as var1
, convert(nvarchar,s1.emigration_yr) as var1_value
, 'yrs_since_emigration' as var2
, convert(nvarchar,years_since_emigration) as var2_value
, 'ethnicity' as var3
, 'Pacific' as var3_value
, 'age20bins' as var4
, convert(nvarchar,s1.age20bins) as var4_value
, 'resident' as measure_type1
, transcat_count as measure1
, 'emigrant_cohort' as measure_type2
, emigrant_cohort as measure2

from 
(select e.yr as emigration_yr
	, t.yr-e.yr as years_since_emigration
	, 20*floor(age/20) as age20bins
	, count(*) as transcat_count
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	on (e.snz_uid=t.snz_uid and t.yr>e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	
	where nz_born_emigrant = 1
	and t.transcat_code = 4
	and Pacific=1
	group by e.yr, t.yr-e.yr, 20*floor(age/20)
	) s1
left join
	(select e.yr as emigration_yr
	, 20*floor(age/20) as age20bins
	,count(*) as emigrant_cohort
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	where nz_born_emigrant=1
	and Pacific=1
	group by e.yr
	, 20*floor(age/20)
	) s2
on (s1.emigration_yr=s2.emigration_yr and s1.age20bins=s2.age20bins)

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 190 as table_nbr
, 'emigration_year' as var1
, convert(nvarchar,s1.emigration_yr) as var1_value
, 'yrs_since_emigration' as var2
, convert(nvarchar,years_since_emigration) as var2_value
, 'ethnicity' as var3
, 'Maori' as var3_value
, 'age20bins' as var4
, convert(nvarchar,s1.age20bins) as var4_value
, 'resident' as measure_type1
, transcat_count as measure1
, 'emigrant_cohort' as measure_type2
, emigrant_cohort as measure2

from 
(select e.yr as emigration_yr
	, t.yr-e.yr as years_since_emigration
	, 20*floor(age/20) as age20bins
	, count(*) as transcat_count
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	on (e.snz_uid=t.snz_uid and t.yr>e.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	
	where nz_born_emigrant = 1
	and t.transcat_code = 4
	and Maori=1
	group by e.yr, t.yr-e.yr, 20*floor(age/20)
	) s1
left join
	(select e.yr as emigration_yr
	, 20*floor(age/20) as age20bins
	,count(*) as emigrant_cohort
	from #emigration e
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (e.snz_uid=b.snz_uid and e.yr=b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
	on (e.snz_uid=d.snz_uid)
	where nz_born_emigrant=1
	and Maori=1
	group by e.yr
	, 20*floor(age/20)
	) s2
on (s1.emigration_yr=s2.emigration_yr and s1.age20bins=s2.age20bins)

--remove nulls

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]
where table_nbr=190
and (measure2 is null or var4_value is null)

-----------------------------------------
-- OE tables
-----------------------------------------

-- table 20 - current residents by age and historical years overseas


drop table if exists #oe

select distinct t.snz_uid, t.yr, max(OE) as OE_years
into #oe

from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
left join
	(
	select	snz_uid
	, yr
	,row_number() over (partition by snz_uid order by yr) as OE
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat]
	where transcat_code = 12
	) s
on (t.snz_uid = s.snz_uid and t.yr>s.yr)

group by t.snz_uid, t.yr

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=20;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 20 as table_nbr
, 'yr' as var1
, convert(nvarchar,t.yr) as var1_value
, 'age_5bins' as var2
, convert(nvarchar,5*floor(age/5)) as var2_value
, 'OE_years' as var3
, convert(nvarchar,o.OE_years) as var3_value
, 'transcat' as var4
, convert(nvarchar,tl.transcat) as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2


from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
left join  #OE o
on (t.snz_uid = o.snz_uid and t.yr = o.yr)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat_code_lookup] tl
on (t.transcat_code = tl.transcat_code)
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
on (t.snz_uid = b.snz_uid and t.yr = b.yr)
where t.transcat_code in (4,12)

group by t.yr,  5*floor(age/5), OE_years, tl.transcat


-- table 21 - income distribution of those aged 40-45 by OE status

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=21;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 21 as table_nbr
, 'yr' as var1
, convert(nvarchar,yr) as var1_value
, 'income_bin' as var2
, convert(nvarchar,income_bin) as var2_value
, 'OE_years' as var3
, convert(nvarchar,OE) as var3_value
, null as var4
, null as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2

from
	(
	select t.snz_uid
	, t.yr
	, case when market_income is null then 0 else 10000*floor(market_income/10000) end as income_bin
	, case	when oe_years is null then 'No OE'
			when oe_years <3 then '1-2 Year OE'
			when oe_years <5 then '3-4 Year OE'
			when oe_years <10 then '5-9 Year OE'
			when oe_years >=10 then '10+ Year OE'
			else 'Unknown'
			end as OE

	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TaxInc_2000_24_202506_SC] i
	on (t.snz_uid = i.snz_uid and t.yr = i.taxyr)
	left join #oe o
	on (t.snz_uid = o.snz_uid and t.yr=o.yr)

	where 5*floor(age/5) = 40
	and t.transcat_code = 4
	and t.yr = 2023
	and transcat_change = 0
	) setup

group by yr, income_bin,OE


-- table 22 - tax paid by those aged 40-45 by OE status

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=22;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 22 as table_nbr
, 'yr' as var1
, convert(nvarchar,yr) as var1_value
, 'income_bin' as var2
, convert(nvarchar,tax_bin) as var2_value
, 'OE_years' as var3
, convert(nvarchar,OE) as var3_value
, null as var4
, null as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2

from
	(
	select t.snz_uid
	, t.yr
	, case	when marketinc_tax_calc is null then 0 
			when marketinc_tax_calc = 0 then 0
			else 1000*floor(marketinc_tax_calc/1000)+500 
			end as tax_bin
	, case	when oe_years is null then 'No OE'
			when oe_years <3 then '1-2 Year OE'
			when oe_years <5 then '3-4 Year OE'
			when oe_years <10 then '5-9 Year OE'
			when oe_years >=10 then '10+ Year OE'
			else 'Unknown'
			end as OE

	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_transcat] t
	left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
	on (t.snz_uid = b.snz_uid and t.yr = b.yr)
	left join [IDI_Sandpit].[DL-MAA2013-16].[TaxInc_2000_24_202506_SC] i
	on (t.snz_uid = i.snz_uid and t.yr = i.taxyr)
	left join #oe o
	on (t.snz_uid = o.snz_uid and t.yr=o.yr)

	where 5*floor(age/5) = 40
	and t.transcat_code = 4
	and t.yr = 2023
	and transcat_change = 0
	) setup

group by yr, tax_bin,OE


-- table 23 - Emigration detail for 2018 for fiscal analysis

drop table if exists #tertiary_yrs

select
d.snz_uid
, count (distinct moe_crs_year_nbr) as tertiary_yrs
into #tertiary_yrs
from [IDI_Sandpit].[DL-MAA2013-16].[TimH_demographics] d
join moe_clean.course c
on (d.snz_uid = c.snz_uid)
where moe_crs_year_nbr <=2018
and birth_yr >1988
group by d.snz_uid



delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration] where table_nbr=23;

insert into [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]

select 23 as table_nbr
, 'age' as var1
, convert(nvarchar,b.age) as var1_value
, 'tertiary_yrs' as var2
, case when tertiary_yrs is null then convert(nvarchar, 0) else convert(nvarchar, tertiary_yrs) end as var2_value
, null as var3
, null as var3_value
, null as var4
, null as var4_value
, 'count' as measure_type1
, count(*) as measure1
, null as measure_type2
, null as measure2

from #emigration e
left join [IDI_Sandpit].[DL-MAA2013-16].[TimH_base] b
on e.snz_uid = b.snz_uid and e.yr=b.yr
left join #tertiary_yrs t
on (b.snz_uid = t.snz_uid)
left join 
	(select snz_uid
	from [IDI_Sandpit].[DL-MAA2013-16].[TimH_NZ_residence]
	where yr=2024
	and nine_twelve = 1
	) r
on (e.snz_uid = r.snz_uid)
where nz_born_emigrant = 1
and e.yr=2018
and b.age<30
and r.snz_uid is null
-- exclude people who were resident in NZ in 2024
	
group by b.age, t.tertiary_yrs

--remove nulls according to microdata release policy

delete from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]
where measure_type2 is not null
and measure2 is null

-- print for export to excel

select * from [IDI_Sandpit].[DL-MAA2013-16].[TimH_emigration]
order by table_nbr, var1_value, var2_value, var3_value, var4_value



