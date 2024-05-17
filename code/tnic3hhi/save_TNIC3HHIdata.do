global fpath "C:/Users/ttshuang/Dropbox/Phil/Irene Tan/Variable_Construction" 
cd "$fpath/data/"

import delimited "TNIC3HHIdata/TNIC3HHIdata.txt", clear

* The data is ready to merge with the sample firm by gvkey and calendar year (datadate year)

save "$fpath/data/TNIC3HHIdata.dta", replace