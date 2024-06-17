## Details about the matching process

Here documents the detailed step to match compustats segment data and HH datasets. (Mostly related to code `match_compseg_hh.py`)

### INPUT

1. `data\firm_info\compustats_segment_00_21.dta`
    - segment data from compustat from 2000-2021
    - output from running first part of `code\competition\competition_variable.sas`
2. `data\hh_dataset\publicfirm_site.dta`
    - processed firm-site-year level data provided by Lisa
    - already matched with gvkey
    - sample period is 2001-2009

### Sample Period

2001-2008 (as the raw data, which we retrieve industry information, is until 2008)

### Algorithm

The current many-to-many matching is soly based on 2-digit SIC code - that is to group the segments in COMPUSTATS and sites in HH with the same 2-digit SIC code. Thus the output is a list-to-list pair (i.e., list of segments is matched to the list of sites). The master matching file is `data\hh_dataset\hh_comp_match_sic2d.csv`
