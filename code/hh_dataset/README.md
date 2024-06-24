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

**Description**: We match the compustats segment with Harte-Hanks (HH) site data to get the estimated internal information system variables from HH dataset. The matching is at firm-fyear-industry level. Specifically, for a firm in a fiscal year, we match its segments with the sites in the same 2-digit SIC code. The matching is a many-to-many (throughout the description, we refer the matching as segments-to-sites. i.e., one-to-many refers to one segment to many sites) matching. We made some assumptions to aggregate the internal IT varaibles: 

1. for one-to-one, we keep the continuous variable and indicator as the same as the those of the site 
2. for one-to-many senario, we sum the continuous variable and keep the max of the indicators; 
3. for many-to-one, we assign all segments the same value for all variables as the single site. 
4. for many-to-many, we sum the continuous variable and keep the max of the indicators and assign all segments the same value for all variables as the aggregated value.

The master matching file is `data\hh_dataset\hh_comp_match_sic2d.csv`

Some SIC2D matching statistics (which can also be find in issue #3):

```bash
unique gvkey if has_match==1
Number of unique values of gvkey is  2344
Number of records is  29194


# Matching Type

  match_type |      Freq.     Percent        Cum.
-------------+-----------------------------------
many-to-many |     14,963       51.25       51.25
 many-to-one |      3,144       10.77       62.02
 one-to-many |      7,389       25.31       87.33
  one-to-one |      3,698       12.67      100.00
-------------+-----------------------------------
       Total |     29,194      100.00


# Matching rate (firm-year level) within HH datasets

        has_match  total_gvkey  has_match_proportion
fyear
2001.0         1807.0             2081              0.868333
2002.0         1738.0             1997              0.870305
2003.0         1728.0             1948              0.887064
2004.0         1919.0             2140              0.896729
2005.0         1831.0             2055              0.890998
2006.0         1733.0             1945              0.891003
2007.0         1605.0             1817              0.883324
2008.0         1514.0             1722              0.879210
```


**Details of coding**: (functions in the python file)

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
- After getting `data\hh_dataset\hh_comp_match_sic2d.csv`, we create segment-level file (using `seg_level_IT.py`) and the output is `data\hh_dataset\match\seg_site_match_sic2d.dta`
