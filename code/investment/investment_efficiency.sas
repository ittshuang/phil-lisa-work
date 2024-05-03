/* ********************************************************************************* */
/* Summary   : Construct Investment efficiency in Goodman et al.,  2014              */
/* Date      : April 24, 2024                                                        */
/* Author    : Irene Tan                                                             */
/* Variables : - CAPEX = Capital + R&D + Advertising from Compustats                 */
/*             - Lag Tobin's Q, CFFO, lag asset growth, lag investment               */
/*             - Sample Period: 2000 to 2020						                 */
/* ********************************************************************************* */

/* Set the Date Range */
%let BEGDATE=01JAN1999; * for calculating lag term; 
%let ENDDATE=31DEC2020;

/* Step1. Extract Compustat Sample & Calculate Tobin's Q, Investment */
/* ***************************************************************** */
data comp1;
set comp.funda(keep=gvkey datadate DATAFMT INDFMT CONSOL POPSRC fyear fyr 
					seq pstkrv pstkl pstk txdb itcb 
					capx xrd xad ceq csho prcc_c oancf re at act ni sale lt);
					
where datadate between "&BEGDATE"d and "&ENDDATE"d 
and DATAFMT='STD' and INDFMT='INDL' and CONSOL='C' and POPSRC='D';
if seq>0; /* Keep Companies with Existing Shareholders' Equity */

/* Calculate Tobin's Q*/
pref = coalesce(pstkrv,pstkl,pstk); /* Preferred Stock */
BE = sum(seq, txdb, itcb, -pref); /* BE = Stockholders Equity + Deferred Taxes + Investment Tax Credit - Preferred Stock */
ME = prcc_c*csho; /* Calculate Market Value of Equity at Year End */

if BE>0 then MtB = ME / BE; /* Calculate Market-to-Book Ratio */
tobin_q = (at + ME - BE) / at; /* Calculate Tobin's Q */

/* Calculate Capital Investment as the sum of CAPEX, R&D and advertising expenditure*/
if missing(capx) then capx=0; if missing(xrd) then xrd=0; if missing(xad) then xad=0; /* Set missing to zero */
investment = capx + xrd + xad;

label datadate = "Fiscal Year End Date";
label BE = "Book Value of Equity";
label ME = "Market Value of Equity";
label MtB= "Market-to-Book Ratio";
label tobin_q ="Tobin's Q";

drop DATAFMT INDFMT CONSOL POPSRC;
run;

* add industry;
proc sql;
create table comp2
	as select a.*, b.sic
	from comp1 a left join comp.company b
		on a.gvkey = b.gvkey
		order by gvkey, datadate;
quit; 

proc sort data=comp2;by gvkey datadate;run;

/* Step 2. Create Lagged Variables and Calculate Asset Growth for Regression Analysis*/
/* ********************************************************************************* */
data comp3;
set comp2;
by gvkey fyear;

/* Create lagged variable and calculate asset growth */
lag_assets = lag(at);
if first.gvkey then lag_assets = .;

if lag_assets > 0 then asset_growth = (at - lag_assets) / lag_assets;
else asset_growth = .;

lag_tobinq = lag(tobin_q);
lag_investment = lag(investment);
lag_asset_growth = lag(asset_growth);

/* Reset lagged variables for the first year of each company */
if first.gvkey then do;
    lag_tobinq = .;
    lag_asset_growth = .;
    lag_investment = .;
end;

if fyear >= 2000; /* only keep sample period while drop 1999 */
sic2d = substr(put(sic, best.), 1, 2); /* Create the first two digits of SIC code */
run;
proc sort data=comp3; by gvkey fyear; run;

* ensure the obs in a industry sic2d is greater than 30;
proc sql;
    create table final as
    select *
    from comp3
    where lag_tobinq is not missing and oancf is not missing 
      and lag_asset_growth is not missing and lag_investment is not missing
    group by sic2d
    having count(gvkey) >= 30;
quit;

* Add cik;

data gvkey2cik;
set wrdssec.wciklink_gvkey(keep=gvkey cik datadate1 datadate2);
if missing(gvkey) then delete;
if missing(cik) then delete;
run;

proc sql;
create table final2 as 
select a.*, b.cik
from final a left join gvkey2cik b
on a.gvkey = b.gvkey;
where datadate1 <= a.datadate <= datadate2;
quit;

data final2;
set final2;
if missing(gvkey) then delete;
if missing(cik) then delete;
run;
proc sort data=final2; by gvkey fyear; run;

* Export this compustat dta; 
proc export data=final2
    outfile="/home/uchicago/irenetan/lisa/data/investment_efficiency.dta"
    dbms=dta replace;
run;




























