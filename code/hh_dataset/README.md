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

2001-2008 (as the raw data, where we retrieve industry information, is until 2008)

### Algorithm

**Summary**: The current many-to-many matching is soley based on 2-digit SIC code - that is to group the segments in COMPUSTATS and sites in HH with the same 2-digit SIC code. Thus the output is a list-to-list pair (i.e., list of segments is matched to the list of sites). The master matching file is `data\hh_dataset\hh_comp_match_sic2d.csv`

**Details**: (functions in the python file)

- `read_compustats_seg()`: where we read and process the compustats segment level data. As some firms will report segment info in multiple ways, we only keep one category and the priority to keep a certain categories is defined in the `priority` dictionary.
- `return_hh_sic()`: We retrieve the sic code from raw HH datasets.
- `read_hh_site()`: read the processed hh site data and merge it with the sic code from `return_hh_sic()`
- The key code line for the matching would be:

```python
# Group compustats segment data
comp_seg_ind_group2 = comp_seg.groupby(['fyear','gvkey','SICGRP']).agg(
    {"sid":list, 'seg_sale':'sum', 'emps':'sum'}
    ).reset_index().rename(columns={'SICS1':'siccode'})

# Group HH_site data
hh_site_group2 = hh_site.groupby(['fyear','gvkey','SICGRP']).agg({
        "siteid":list, 
        'reven':'sum',
        'emple':'sum',
        'internet':'max', # the three are indicator variable so get the max
        'intranet':'max', 
        'intranet_MS':'max', 
        'totpc':'sum', # get the sum for total PC
        'ahq': 'max', # if  the matched sites includes HQ
        }).reset_index()

```

- After getting `data\hh_dataset\hh_comp_match_sic2d.csv`, we create segment-level file (using `seg_level_IT.py`)
