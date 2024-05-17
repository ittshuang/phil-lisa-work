global fpath "C:/Users/ttshuang/Dropbox/Phil/Irene Tan/Variable_Construction" 
cd "$fpath/data/"

use "investment_efficiency.dta", clear

* Run the regression aggregate investment with sic2d and year FE
reghdfe investment lag_tobinq oancf lag_asset_growth lag_investment, absorb(sic2d fyear) cluster(sic2d)

* Predict the expected investment values
predict expected_investment

* Calculate the unexpected investment as the absolute difference
gen unexpected_investment = abs(investment - expected_investment)

* Calculate the median of unexpected investment and create the investment efficiency variable
egen median_uninv = median(unexpected_investment)
gen investment_efficiency = (unexpected_investment <= median_uninv)

* Calculate the industry-year median of unexpected investment
sort sic2d fyear
egen median_uninv_ind_year = median(unexpected_investment), by(sic2d fyear)
gen investment_efficiency2 = (unexpected_investment <= median_uninv_ind_year)

sum investment_efficiency investment_efficiency2

save "$fpath/data/investment_efficiency_final.dta", replace
