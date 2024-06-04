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

# read data and keep relevant columns: sales are in millions
comp_seg = pd.read_stata(COMPSEG_FILE)[
    ['gvkey','fyear','total_sales','sid','stype','snms','emps','seg_sale','SICS1','NAICSS1']
    ].sort_values(['gvkey','fyear']).reset_index(drop=True)

# Only keep year 2001 to 2009 as the HH data is from 2001 to 2009
comp_seg = comp_seg[(comp_seg['fyear']<=2009) & (comp_seg['fyear']>=2001)].reset_index(drop=True)
# only keep one stype based on the priority
priority = {
    'GEOSEG': 2,
    'BUSSEG': 1,
    'OPSEG': 3,
    'STSEG': 4
    }

comp_seg['priority'] = comp_seg['stype'].map(priority)
comp_seg = filter_high_priority(comp_seg)

# read processed hh_site data
hh_site = pd.read_stata(HH_FILE)[
    ['gvkey','year','corpid','siteid','emple','reven','avg_reven'] # from documentation: reven is in millions
    ].sort_values(['gvkey','year']).reset_index(drop=True)


# No industry for this processed industry data - add 2004 data from the raw data
# start from raw hh 2004 data - add sic code 
raw_2004 = pd.read_csv(r'data\hh_dataset\USA_2004\Hist2004_SITEDESC.txt', delimiter="\t", encoding='latin1')
cols = ['SITEID','CORPHDQ','SICCODE','SICGRP',"NAICS"]
raw_2004 = raw_2004[cols]
raw_2004.columns = ['siteid','corphdq','siccode','SICGRP','NAICSS1']

# only look at 2004 data
hh_site_2004 = hh_site[hh_site['year']==2004]
# link with hh_site 2004 data
hh_site_2004 = hh_site_2004.merge(raw_2004, on='siteid', how='left' )
hh_site_2004['sic2d'] = hh_site_2004['siccode'].astype(str).str[:2].astype(int) 


# map ind group for compustats data
comp_seg_2004 = comp_seg[comp_seg['fyear']==2004]
comp_seg_2004['sic2d'] = comp_seg_2004['SICS1'].astype(str).str[:2].astype(int) 
comp_seg_2004['SICGRP'] = comp_seg_2004['sic2d'].apply(map_industry)

comp_seg_2004_ind_group = comp_seg_2004.groupby(['gvkey','sic2d']).agg({"sid":list}).reset_index().rename(columns={'SICS1':'siccode'})
hh_site_2004_group = hh_site_2004.groupby(['gvkey','sic2d']).agg({"siteid":list}).reset_index()

result = comp_seg_2004_ind_group.merge(hh_site_2004_group, on=['gvkey','sic2d'],how='outer').sort_values(['gvkey','sic2d'])
result['SICGRP'] = result['sic2d'].apply(map_industry)
result.to_csv(r'data/hh_dataset/hh_comp_match_sic2d_2004.csv', index=False)

'''
# Average to firm-site or firm-segment level
hh_site = hh_site.groupby(['gvkey', 'siteid']).agg({'emple':'mean', 'reven':'mean'}).reset_index().sort_values(['gvkey','reven'])
comp_seg = comp_seg.groupby(['gvkey', 'sid']).agg({'emps':'mean', 'seg_sale':'mean'}).reset_index().sort_values(['gvkey','seg_sale'])


# Create bins 
hh_site['emple_bin'] = pd.cut(hh_site['emple'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])
hh_site['reven_bin'] = pd.cut(hh_site['reven'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])

# Create bins for segment sales in comp_seg
comp_seg['seg_sale_bin'] = pd.cut(comp_seg['seg_sale'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])
comp_seg['emps_bin'] = pd.cut(comp_seg['emps'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])
'''
# 2nd data: there are siteid in this df that are missing the HH_FILE1... (ask lisa about this?)
'''
hh_site2 = pd.read_stata(HH_FILE2)[
    ['gvkey','site_year','siteid',
     'zipcode', 'county',
     'sic3', 'siccode', 'naics3', 'naics',]
    ].sort_values(['gvkey', 'site_year']).reset_index(drop=True)
 '''


# merge the two
#hh_site = hh_site.merge(hh_site2,
#                        left_on=['siteid','year'],
#                        right_on=['siteid', 'site_year'],
#                        how='left'
#                        ).dropna(subset='siccode').reset_index(drop=True) 
