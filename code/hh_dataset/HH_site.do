* Code to explore and prepare site-level data for matching with compustats data

* For mac
global fpath1_1 "/Users/irene/Library/CloudStorage/Dropbox/Phil/Irene Tan/HH_dataset/Data" 

*global fpath1_1 "C:\Users\\ttshuang\Dropbox\Phil\Irene Tan\HH_dataset\Data" 
cd `fpath1_1'

* Firm-level
import delimited "${fpath1_1}\USA_2004\Hist2004_CORP.txt", clear 

* Site-level
import delimited "${fpath1_1}/USA_2004/Hist2004_SITEDESC.txt", clear 
keep siteid company fiscl sic3 siccode sicgrp naics3 naics emple reven corpid turnover_local icorpid
gsort -corpid siteid siccode


use "/Users/irene/Library/CloudStorage/Dropbox/Phil/Irene Tan/Variable_Construction/data/compustats_segment_00_21.dta", clear
