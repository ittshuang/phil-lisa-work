'''Code to match the compustata segment data with HH Datasets'''

import pandas as pd 
import os
import numpy as np

COMPSEG_FILE = r'data\firm_info\compustats_segment_00_21.dta'
HH_FILE = r'data\hh_dataset\publicfirm_site.dta'
#HH_FIL2E = r"data\hh_dataset\2001_2009_site_clean_data.dta"


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

def filter_high_priority(df):
    min_priority = df.groupby(['gvkey', 'fyear'])['priority'].transform('min')
    return df[df['priority'] == min_priority]


def map_industry(code):
    for code_range, abbr in industry_map.items():
        if code in code_range:
            return abbr
    return None

def read_compustats_seg():
    # define the segment division priority
    priority = {
        'GEOSEG': 2,
        'BUSSEG': 1,
        'OPSEG': 3,
        'STSEG': 4
        }
    
    # read data and keep relevant columns: sales are in millions
    comp_seg = pd.read_stata(COMPSEG_FILE)[
        ['gvkey','fyear','total_sales','sid','stype','snms','emps','seg_sale','SICS1','NAICSS1']
        ].sort_values(['gvkey','fyear']).reset_index(drop=True)

    # Only keep year 2001 to 2009 as the HH data is from 2001 to 2009
    comp_seg = comp_seg[(comp_seg['fyear']<=2009) & (comp_seg['fyear']>=2001)].reset_index(drop=True)
    
    # only keep one stype based on the priority
    comp_seg['priority'] = comp_seg['stype'].map(priority)
    comp_seg = filter_high_priority(comp_seg)
    comp_seg['sic2d'] = comp_seg['SICS1'].astype(str).str[:2].astype(int) 
    comp_seg['SICGRP'] = comp_seg['sic2d'].apply(map_industry)
    return comp_seg

def return_hh_sic():
    raw_list = []
    for year in range(2001,2009):
        if year in range(2001,2005):
            sitedesc_path = f'data\\hh_dataset\\USA_{year}\\Hist{year}_SITEDESC.txt'
        else: 
            sitedesc_path = f'data\\hh_dataset\\USA_{year}\\Hist{year}_sitedesc.txt'
        # read in site descriptive data
        raw = pd.read_csv(sitedesc_path, delimiter="\t", encoding='latin1')
        raw['fyear'] = year
        cols = ['SITEID','fyear','CORPHDQ','SICCODE','SICGRP',"NAICS"]
        raw = raw[cols].drop_duplicates(subset=['SITEID'])
        raw.columns = ['siteid','fyear','corphdq','siccode','SICGRP','NAICSS1']
        raw_list.append(raw)
    sic_final = pd.concat(raw_list).reset_index(drop=True)
    return sic_final

def read_hh_site():
    # read processed hh_site data
    hh_site = pd.read_stata(HH_FILE)[
        ['gvkey','year','corpid','siteid','emple','reven','avg_reven'] # from documentation: reven is also in millions
        ].sort_values(['gvkey','year']).reset_index(drop=True)

    # No industry for this processed industry data - add industry data from the raw data
    # add sic code 
    hh_sic = return_hh_sic()
    hh_site.rename(columns={'year':'fyear'}, inplace=True)
    # merge with hh_site sic code
    hh_site = hh_site.merge(hh_sic, on=['siteid','fyear'], how='left' )
    hh_site['sic2d'] = hh_site['siccode'].astype(str).str[:2]
    hh_site['sic2d'] = pd.to_numeric(hh_site['sic2d'], errors='coerce')
    hh_site = hh_site.dropna(subset=['sic2d'])
    hh_site['sic2d'] = hh_site['sic2d'].astype(int)
    return hh_site

def main():
    comp_seg = read_compustats_seg()
    hh_site = read_hh_site()

    # group sic2d for compustats data
    comp_seg_ind_group = comp_seg.groupby(['fyear','gvkey','sic2d']).agg({"sid":list, 'seg_sale':'sum', 'emps':'sum'}).reset_index().rename(columns={'SICS1':'siccode'})
    hh_site_group = hh_site.groupby(['fyear','gvkey','sic2d']).agg({"siteid":list, 'reven':'sum','emple':'sum'}).reset_index()

    result = comp_seg_ind_group.merge(hh_site_group, on=['fyear','gvkey','sic2d'],how='outer').sort_values(['gvkey','fyear','sic2d'])
    result['SICGRP'] = result['sic2d'].apply(map_industry)
    cols = ['fyear','gvkey','sic2d','SICGRP','sid','seg_sale','emps','siteid','reven','emple']
    result = result[cols]
    result.columns = ['fyear','gvkey','sic2d','SICGRP','seg_id','seg_sale','seg_emps','site_id','site_reven','site_emple']
    result_high = result.dropna(subset=['seg_id','site_id'])
    result.to_csv(r'data\hh_dataset\hh_comp_match_sic2d.csv', index=False)
    result_high.to_csv(r'data\hh_dataset\hh_comp_match_sic2d_high.csv', index=False)
    
    # group SICGRP for compustats data
    comp_seg_ind_group2 = comp_seg.groupby(['fyear','gvkey','SICGRP']).agg({"sid":list, 'seg_sale':'sum', 'emps':'sum'}).reset_index().rename(columns={'SICS1':'siccode'})
    hh_site_group2 = hh_site.groupby(['fyear','gvkey','SICGRP']).agg({"siteid":list, 'reven':'sum','emple':'sum'}).reset_index()

    result2 = comp_seg_ind_group2.merge(hh_site_group2, on=['fyear','gvkey','SICGRP'],how='outer').sort_values(['gvkey','fyear'])
    cols = ['fyear','gvkey','SICGRP','sid','seg_sale','emps','siteid','reven','emple']
    result2 = result2[cols]
    result2.columns = ['fyear','gvkey','SICGRP','seg_id','seg_sale','seg_emps','site_id','site_reven','site_emple']
    result2_high = result2.dropna(subset=['seg_id','site_id'])
    result2.to_csv(r'data\hh_dataset\hh_comp_match_sicgrp.csv', index=False)
    result2_high.to_csv(r'data\hh_dataset\hh_comp_match_sicgrp_high.csv', index=False)
    


main()
