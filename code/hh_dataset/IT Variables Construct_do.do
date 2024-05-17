
global fpath1_1 "/Users/yl4689/Dropbox/Columbia/Working Paper/Ongoing Project/With_Suzie/Consistent Inter-External Communicators/Data/HarteHanks_purchased/" 

cd "fpath1_1"


**********************************************************************************
* 1. Get PC indicator from each year's survey
**********************************************************************************
foreach year in 2001 2002 2003 2004 {
	import delimited "${fpath1_$user}/USA_`year'/Hist`year'_SYSTEM.txt", clear 
	merge 1:1 siteid using "${fpath2_$user}/management forecast/data_regression_temp/`year'_site.dta"
	drop if _m==2
	capture drop _m
	capture gen year = `year'
	keep siteid year corpid emple company totpc totdesk totport totserver totwks totprt totnodes
	save "${fpath2_$user}/management forecast/data_regression_temp/`year'_it_sys_site.dta",replace
}

foreach year in 2005 2006 2007 2008 2009  {
	import delimited "${fpath1_$user}/USA_`year'/Hist`year'_system.txt", clear 
	merge 1:1 siteid using "${fpath2_$user}/management forecast/data_regression_temp/`year'_site.dta"
	drop if _m==2
	capture drop _m
	capture gen year = `year'
	keep siteid year corpid emple company totpc totdesk totport totserver totwks totprt totnodes
	save "${fpath2_$user}/management forecast/data_regression_temp/`year'_it_sys_site.dta",replace
}


*2010
import delimited "${fpath1_$user}/USA_2010/Hist2010_Technology.txt", clear 
merge 1:1 siteid using "${fpath2_$user}/management forecast/data_regression_temp/2010_site.dta"
drop if _m==2
capture drop _m
capture gen year = 2010
keep siteid year ent_id emple totpc
save "${fpath2_$user}/management forecast/data_regression_temp/2010_it_sys_site.dta",replace

*2012
import delimited "${fpath1_$user}/USA_2012/Hist2012_TECHNOLOGY.txt", clear 
merge 1:1 siteid using "${fpath2_$user}/management forecast/data_regression_temp/2012_site.dta"
drop if _m==2
capture drop _m
capture gen year = 2012
keep siteid year ent_id emple pcs
rename pcs totpc
save "${fpath2_$user}/management forecast/data_regression_temp/2012_it_sys_site.dta",replace

foreach year in 2011 2013 2014 2015{
	import delimited "${fpath1_$user}/USA_`year'/Hist`year'_TECHNOLOGY.txt", clear 
	merge 1:1 siteid using "${fpath2_$user}/management forecast/data_regression_temp/`year'_site.dta"
	drop if _m==2
	capture drop _m
	capture gen year = `year'
	keep siteid year ent_id emple pcs
	rename pcs totpc
	save "${fpath2_$user}/management forecast/data_regression_temp/`year'_it_sys_site.dta",replace
}


foreach year in 2016 2017 2018 2019{
	import delimited "${fpath1_$user}/USA_`year'/TechnologyTotals.txt", clear 
	merge 1:1 siteid using "${fpath2_$user}/management forecast/data_regression_temp/`year'_site.dta"
	drop if _m==2
	capture drop _m
	capture gen year = `year'
	keep siteid year ent_id emple pcs
	rename pcs totpc
	save "${fpath2_$user}/management forecast/data_regression_temp/`year'_it_sys_site.dta",replace
}




**********************************************************************************
* 2. Append all together and merge on company id
**********************************************************************************
*--- 2-1 Append all together
use "${fpath2_$user}/management forecast/data_regression_temp/2001_it_sys_site.dta",clear
forvalues year = 2002(1)2019{
	append using "${fpath2_$user}/management forecast/data_regression_temp/`year'_it_sys_site.dta"
}
save "${fpath2_$user}/management forecast/data_regression_temp/it_sys_site_2001_2019.dta",replace


---- Perseonally  I don't think below is needed. We can do this at the sample construction part

*--- 2-2 Merge on company ID
use "${fpath2_$user}/management forecast/data_regression_temp/it_sys_site_2001_2019.dta",clear
*2-1-1 get corpid : For the year 2001-2009  --- All unmatched are those without corpid;    531 missing is filled
merge m:1 siteid year using "${fpath2_$user}/management forecast/data_regression_temp/siteid_corpid_link.dta"
drop if _m == 2
drop _m
replace corpid = corpid_0109 if corpid==. | corpid == 0

*2-1-2 get ent_id: For the year 2010-2019 ---                112,912 missing is filled
merge m:1 siteid year using "${fpath2_$user}/management forecast/data_regression_temp/siteid_entid_link.dta"
drop if _m == 2
drop _m
replace ent_id = ent_id_1019 if substr(ent_id,1,1)==" " & substr(ent_id_1019,1,1)!=""

*2-1-3 Drop the firms without a valid ID
drop if substr(ent_id,1,1)==" " & corpid == .
drop if substr(ent_id,1,1)=="" & corpid == .
drop if substr(ent_id,1,1)==" " & corpid == 0
drop if substr(ent_id,1,1)=="" & corpid == 0
save "${fpath2_$user}/management forecast/data_regression_temp/it_sys_site_2001_2019_clean.dta",replace




use "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_clean.dta",clear
xtset siteid year
sort siteid year
tsfill, full
global non_dummies totwks totpc totdesk totport totprt totnodes totserver 

forvalues i = 2002(1)2019{
	foreach var in totpc corpid emple{
		replace `var' = l1.`var' if `var' == . & year == `i'
	}
}
gen avg_pc = totpc/emple
save "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_v1.dta",replace

use "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_clean.dta",clear
xtset siteid year
sort siteid year
tsfill, full
global non_dummies totwks totpc totdesk totport totprt totnodes totserver 

forvalues i = 2018(-1)2001{
	foreach var in totpc corpid emple{
		replace `var' = f1.`var' if `var' == . & year == `i'
	}
}
gen avg_pc = totpc/emple
save "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_v2.dta",replace


use "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_v1.dta",clear
drop if avg_pc ==.
save "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_v1.dta",replace
drop corpid_0109 ent_id_1019
merge m:1 siteid using "$path2/management forecast/data_regression_temp/siteid_corpid_link.dta"
drop if _m == 2
drop _m
replace corpid = corpid_0109 if corpid==. | corpid == 0
*get ent_id
merge m:1 siteid using "$path2/management forecast/data_regression_temp/siteid_entid_link.dta"
drop if _m == 2
drop _m
replace ent_id = ent_id_1019 if substr(ent_id,1,1)!="E" & substr(ent_id_1019,1,1)=="E"
drop if substr(ent_id,1,1)!="E" & corpid == .
drop if substr(ent_id,1,1)!="E" & corpid == 0
* for firms before 2009, use corpid to get gvkey
replace ent_id = "" if corpid!=. & year<=2009
* for firms after 2010, use ent_id to get gvkey
replace corpid = . if substr(ent_id,1,1)=="E" & year>2009

**get gvkey for corpids
joinby corpid using "$path2/public_firm_name/Data Temp/merged2_HH_01_09.dta", unmatched(master) 
drop if _m == 2
drop _m
capture drop index
duplicates drop
rename gvkey gvkey_1

joinby ent_id using "$path2/public_firm_name/Data Temp/merged2_HH_10_19.dta", unmatched(master) 
drop _m
capture drop index
duplicates drop

tostring gvkey,gen(gvkey2)
replace gvkey2 = "0" + gvkey2 if strlen(gvkey2) == 5
replace gvkey2 = "00" + gvkey2 if strlen(gvkey2) == 4
replace gvkey2 = "000" + gvkey2 if strlen(gvkey2) == 3
drop gvkey
rename gvkey2 gvkey

replace gvkey = gvkey1 if gvkey == ""

drop if gvkey==""


collapse avg_pc=avg_pc [aweight=emple] ,by(gvkey year)

save "$path2/management forecast/data_regression_temp/it_sys_firm_2001_2019_v1.dta",replace

use "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_v2.dta",clear
drop if avg_pc ==.
save "$path2/management forecast/data_regression_temp/it_sys_site_2001_2019_v2.dta",replace
drop corpid_0109 ent_id_1019
merge m:1 siteid using "$path2/management forecast/data_regression_temp/siteid_corpid_link.dta"
drop if _m == 2
drop _m
replace corpid = corpid_0109 if corpid==. | corpid == 0
*get ent_id
merge m:1 siteid using "$path2/management forecast/data_regression_temp/siteid_entid_link.dta"
drop if _m == 2
drop _m
replace ent_id = ent_id_1019 if substr(ent_id,1,1)!="E" & substr(ent_id_1019,1,1)=="E"
drop if substr(ent_id,1,1)!="E" & corpid == .
drop if substr(ent_id,1,1)!="E" & corpid == 0
* for firms before 2009, use corpid to get gvkey
replace ent_id = "" if corpid!=. & year<=2009
* for firms after 2010, use ent_id to get gvkey
replace corpid = . if substr(ent_id,1,1)=="E" & year>2009
save "$path2/public_firm_name/temp_20220609.dta",replace

use "$path2/public_firm_name/temp_20220609.dta",clear
keep if corpid!=. & corpid!=0
**get gvkey for corpids
joinby corpid using "$path2/public_firm_name/Data Temp/merged2_HH_01_09.dta", unmatched(master) 
drop if _m == 2
drop _m
capture drop index
duplicates drop
drop if gvkey==""
save "$path2/public_firm_name/temp1_20220609.dta",replace

forvalues year = 2001(1)2019{
	use "$path2/public_firm_name/temp_20220609.dta",clear
	keep if substr(ent_id,1,1)=="E" & year == `year'
	joinby ent_id using "$path2/public_firm_name/Data Temp/merged2_HH_10_19.dta", unmatched(master) 
	drop _m
	capture drop index
	duplicates drop

	duplicates drop

	save "$path2/public_firm_name/temp2_`year'_20220609.dta",replace
}

forvalues year = 2001(1)2019{
	use "$path2/public_firm_name/temp2_`year'_20220609.dta",replace
	drop if gvkey == ""
	save "$path2/public_firm_name/temp2_`year'_20220609.dta",replace
}


use "$path2/public_firm_name/temp1_20220609.dta",clear
forvalues year = 2001(1)2019{
	append using "$path2/public_firm_name/temp2_`year'_20220609.dta"
}
duplicates drop

collapse avg_pc=avg_pc [aweight=emple] ,by(gvkey year)

save "$path2/management forecast/data_regression_temp/it_sys_firm_2001_2019_v2.dta",replace









