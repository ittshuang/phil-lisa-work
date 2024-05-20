
/* Step 1: Prepare the segment data*/ 

* get the max srcdate for a company in a year;
proc sql;
    create table max_srcdate as
    select gvkey, datadate, max(srcdate) as maxsrcdate
    from compsegd.wrds_segmerged
    group by gvkey, datadate;
quit;


proc sql;
    create table temp1 as
    select a.gvkey, a.conm, a.fic, a.datadate, a.fyear, 
           a.sale as total_sales, a.capx as total_capx, a.xrd as total_rd, a.at as total_assets, a.ppegt as total_ppe,
           b.gvkey, b.sid, b.sics1, b.naicss1, b.stype, b.datadate, b.srcdate,
           b.emps, b.sales as seg_sale, b.ops as seg_ops, b.oibdps as seg_oibdps, 
           b.rds as seg_rds, b.capxs as seg_capxs, b.ias as seg_ias, b.ppents as seg_ppe
    from comp.funda as a
    inner join compsegd.wrds_segmerged as b
    on a.gvkey = b.gvkey and a.datadate = b.datadate
    inner join max_srcdate m
    on b.gvkey = m.gvkey and b.datadate = m.datadate and b.srcdate = m.maxsrcdate
    where a.fic = 'USA' 
      and b.sics1 is not null 

      and year(b.datadate) between 2000 and 2021;
quit;
proc sort nodupkey data=temp1; by gvkey datadate sid ; run;

* Add firm industry;
proc sql;
create table comp1
	as select a.*, b.sic
	from temp1 a left join comp.company b
		on a.gvkey = b.gvkey
		order by gvkey, datadate;
quit; 

* Export segment data as dta; 
proc export data=comp1
    outfile="/home/uchicago/irenetan/lisa/data/compustats_segment_00_21.dta"
    dbms=dta replace;
run;


/* Step 2: Aggregate financial items for segments with identical SIC codes under the same firm */
proc sql;
create table temp2 as
select gvkey,sic, fyear,sics1,conm,
max(total_sales) as total_sales, 
max(total_capx) as total_capx,
max(total_rd) as total_rd,
max(total_assets) as total_assets,
max(total_ppe) as total_ppe,
sum(seg_sale) as seg_sale_sum,
sum(seg_ops) as seg_ops_sum,
sum(seg_oibdps) as seg_oibdps_sum,
sum(seg_rds) as seg_rds_sum,
sum(seg_capxs) as seg_capxs_sum,
sum(seg_ias) as seg_ias_sum,
sum(seg_ppe) as seg_ppe_sum
from comp1
group by gvkey, fyear, sics1;
quit;
proc sort nodupkey data=temp2; by gvkey fyear sics1 ; run;


/* Aggregate industry sales */
proc sql;
    create table industry_sales as
    select sics1, fyear,
           sum(seg_sale_sum) as industry_sales
    from temp2
    group by sics1, fyear;
quit;

/* Calculate Market Shares */
proc sql;
    create table firm_market_shares as
    select a.sics1, a.fyear, a.gvkey,a.sic,
           a.seg_sale_sum, b.industry_sales,
           a.seg_sale_sum / b.industry_sales as market_share
    from temp2 as a
    left join industry_sales as b
    on a.sics1 = b.sics1 and a.fyear = b.fyear;
quit;

proc sort nodupkey data=firm_market_shares; by gvkey fyear sics1 market_share; run;
proc sort data=firm_market_shares; by sics1 fyear descending market_share; run;
* Calculate the HHI and Four-firm concentration ratio;
data ranked_market_shares;
    set firm_market_shares;
    by sics1 fyear;

    /* Assign ranking within each industry and year */
    if first.fyear then rank = 1;
    else rank + 1;

    /* Retain the needed variables */
    retain rank;
run;

/* Filter for top 4 ranks and prepare for IND-CON4 and IND-HHI calculations */
proc sql;
    create table industry_concentration as
    select gvkey, sics1, fyear, market_share, sic,
        /* Calculate IND-CON4: Sum of market shares of the top 4 firms */
        sum(case when rank <= 4 then market_share else 0 end) as ind_con4,
        /* Calculate IND-HHI: Sum of squared market shares of all firms */
        sum(market_share**2) as ind_hhi
    from ranked_market_shares
    group by sics1, fyear;
quit;
proc sort data=industry_concentration; by gvkey fyear sics1  descending market_share; run;

* Drop hhi and con4 which is not within 0-1;
data industry_concentration;
set industry_concentration;
if ind_con4 = . then delete;
if ind_con4 > 1 then delete;
if ind_hhi = . then delete;
if ind_hhi > 1 then delete;
run;

* Export hhi and con4 as dta; 
proc export data=industry_concentration
    outfile="/home/uchicago/irenetan/lisa/data/hhi_con4_00_21.dta"
    dbms=dta replace;
run;

