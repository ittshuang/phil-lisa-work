# Everything about the HH datasets

- `USA_{YEAR}`: Raw data
-  `Documentation`: Dataset documentation by year
- `Internal IS Variable Summary.xlsx`: Variable category by Irene
- `Public_hh_dataset_kept.dta`: Firm-year level link b/w gvkey and hh
- `publicfirm_site.dta`: firm-site-year level data with gvkey
- `2001_2009_site_clean_data.dta`: firm-site-year level data with more columns (SIC code etc)


output:

- `hh_comp_match_sic2d.csv`: matched results based on sic2d, the many to many output is displayed as seg_id list to site_id list
- `hh_comp_match_sicgrp.csv`: same but matched based on SICGROUP (more relaxing). The map is based on:

    industry_map = {
        range(1, 10): "AG-M-C",
        range(10, 15): "AG-M-C",
        range(15, 18): "AG-M-C",
        range(20, 40): "MANUF",
        range(40, 50): "TR-UTL",
        range(50, 52): "WHL-RT",
        range(52, 60): "WHL-RT",
        range(60, 68): "F-I-RE",
        range(70, 90): "SVCS",
        range(91, 100): "GOVT"
    }

- `seg_site_match.dta`:  Segment level dta, from the `hh_comp_match_sic2d.csv`. The assumption of the expansion is to assign the same site_id list and variables to multiple segment id matched