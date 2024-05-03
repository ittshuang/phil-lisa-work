*=======================================================================================================;
* Code to construct EPS forecast accuracy from IBES;
* Written by Irene at 2024/2/11
*=======================================================================================================;

/* Raw data preparation*/
* ------------------------------------------------------------------------------------------------------------------------------;
data ibes_det1;
set ibes.det_epsus;
where usfirm = 1
	and measure = "EPS"
    and fpi = "1"
	and pdf = "D"; * Use Forecast Year end Diluted EPS;
run;
proc sort nodupkey; by ticker analys anndats fpedats; run; 

* create year and id variable;
data ibes_det1;
set ibes_det1;
if anndats >= anndats_act then delete; * Ensure the forecast date is before the actual announcement date;
if month(fpedats) > 6 then fiscal_year = year(fpedats); * create fiscal year;
else fiscal_year = year(fpedats) - 1;
id = ticker||"_"||analys ||"_"||fiscal_year;
run;
proc sort; by id anndats; run;

* calculate the error = abs(value-actual);
data ibes_det1;
set ibes_det1;
error = abs(value-actual);
drop ACTTIMS REVDATS REVTIMS ANNTIMS ACTTIMS_ACT ANNTIMS_ACT CURR_ACT report_curr CURR CURRFL;
run;


/* Version 1: Divided by Actual 
------------------------------------------------------------------------------------------------------------------------------
accuracy1_1 = abs(actual-value)/abs(actual): the most recent forecast accuracy before actual announcement. ;
accuracy1_2 = E[abs(actual-value)/abs(actual)]: the mean of forecaset accuracy by a year.;
------------------------------------------------------------------------------------------------------------------------------
*/
proc sql;
create table ibes_accuracy1 as
select *, error/abs(actual) as accuracy1_1, mean(error/abs(actual)) as accuracy1_2
from ibes_det1
group by id;
quit;
proc sort; by id anndats; run;

* keep the latest obs;
data ibes_accuracy1;
set ibes_accuracy1;
by id;
if last.id then output;
run;

/* Version 2: divide by year end price
------------------------------------------------------------------------------------------------------------------------------
accuracy2_1 = abs(actual-value)/year-end price: the most recent forecast accuracy before actual announcement. 
accuracy2_2 = E[abs(actual-value)/year-end price]: the mean of forecaset accuracy by a year.
------------------------------------------------------------------------------------------------------------------------------
*/

* price data - get the year-end price;
data ibes_actpsum;
set ibes.actpsum_epsus;
if month(fy0edats) >= 6 then fiscal_year = year(fy0edats); * create fiscal year;
else fiscal_year = year(fy0edats)-1;
id = ticker||"_"||fiscal_year;
run;
proc sort nodupkey; by ticker fy0edats; run;

data ibes_actpsum;
set ibes_actpsum;
by id;
if last.id then output; * keep the last obs of the fyear;
run;

* left join based on year firm;
proc sql;
create table ibes_det2 as
	select a.*, b.price, b.prdays, b.statpers
	from ibes_det1 a left join ibes_actpsum b
	on a.ticker = b.ticker
	and a.fiscal_year = b.fiscal_year;
run;

proc sql;
create table ibes_accuracy2 as
select *, error/price as accuracy2_1, mean(error/price) as accuracy2_2
from ibes_det2
group by id;
quit;

data ibes_accuracy2;
set ibes_accuracy2;
by id;
if last.id then output;
run;

/* Version 3: divide by 3 days prior price (Goodman et al., 2014) 
------------------------------------------------------------------------------------------------------------------------------
accuracy3_1 = abs(actual-value)/3 days prior price: the most recent forecast accuracy before actual announcement. 
accuracy3_2 = E[abs(actual-value)/3 days prior price]: the mean of forecaset accuracy by a year.
------------------------------------------------------------------------------------------------------------------------------
*/

* Extract CRSP data;
data crsp_data;
set crsp.dsf(keep = cusip permno permco date prc);
where prc ne 0;
if prc < 0 then prc=-(prc);
else prc=prc;
run;
proc sort nodupkey; by permno date; run;

proc sql;
	create table ibes_det3 as
	select a.*, b.*
	from ibes_det1 a left join crsp_data b
		on a.cusip = b.cusip
		and a.anndats - 3 = b.date;
quit;

data ibes_det3;
set ibes_det3;
if cusip = "" then delete;
if prc = . then delete;
run;

proc sql;
create table ibes_accuracy3 as
select *, error/prc as accuracy3_1, mean(error/prc) as accuracy3_2
from ibes_det3
group by id;
quit;
proc sort nodupkey; by id anndats; run;

* Keep the latest annual forecast accuracy for accuracy3_1;
data ibes_accuracy3;
set ibes_accuracy3;
by id;
if last.id then output;
run;




/* Merge the three versions*/
proc sql;
create table temp as 
select a.*, b.accuracy2_1, b.accuracy2_2
from ibes_accuracy1 a left join ibes_accuracy2 b
on a.id = b.id;
run;

proc sql;
create table ibes_accuracy as 
select a.*, b.accuracy3_1, b.accuracy3_2
from temp a left join ibes_accuracy3 b
on a.id = b.id;
run;

data ibes_accuracy;
    set ibes_accuracy;
    if cmiss(accuracy1_1, accuracy2_1, accuracy3_1) <3;
run;


/* ADD GVKEY*/
* Try the existing linkage table as pointed https://wrds-www.wharton.upenn.edu/pages/support/support-articles/ibes/merging-ibes-compustat-cusip/;
data ibtic2gvkey;
set comp.security(keep=gvkey ibtic);
if missing(ibtic) then delete;
run;
proc sort nodupkey; by ibtic gvkey; run;

proc sql;
create table ibes_accuracy_final as 
select a.*, b.gvkey
from ibes_accuracy a left join ibtic2gvkey b
on a.ticker = b.ibtic;
quit;

data gvkey2cik;
set wrdssec.wciklink_gvkey(keep=gvkey cik datadate1 datadate2);
if missing(gvkey) then delete;
if missing(cik) then delete;
run;

proc sql;
create table ibes_accuracy_final2 as 
select a.*, b.cik
from ibes_accuracy_final a left join gvkey2cik b
on a.gvkey = b.gvkey;
where datadate1 <= a.anndats_act <= datadate2;
quit;

data ibes_accuracy_final2;
set ibes_accuracy_final2;
if missing(gvkey) then delete;
if missing(cik) then delete;
run;


proc export data=ibes_accuracy_final2
    outfile="/home/uchicago/irenetan/lisa/data/ibes_forecast_accuracy.dta"
    dbms=dta replace;
run;





