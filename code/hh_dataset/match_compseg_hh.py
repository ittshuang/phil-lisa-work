'''Code to match the compustata segment data with HH Datasets'''

import pandas as pd 
import os
import numpy as np

COMPSEG_FILE = r'data\firm_info\compustats_segment_00_21.dta'
HH_FILE = r"data\hh_dataset\publicfirm_site.dta"

# read data and keep relevant columns
comp_seg = pd.read_stata(COMPSEG_FILE)[['gvkey','fyear','total_sales','sid','emps','seg_sale','sic']].sort_values(['gvkey','fyear']).reset_index(drop=True)
hh_site = pd.read_stata(HH_FILE)[['gvkey','year','corpid','siteid','emple','reven','avg_reven']].sort_values(['gvkey','year']).reset_index(drop=True)

# Only keep year 2001 to 2009 as the HH data is from 2001 to 2009
comp_seg = comp_seg[(comp_seg['fyear']<=2009) & (comp_seg['fyear']>=2001)].reset_index(drop=True)
# Only keep the gvkey which are in HH data
comp_seg = comp_seg[comp_seg['gvkey'].isin(hh_site.gvkey.drop_duplicates().tolist())].reset_index(drop=True)

# Average to firm-site or firm-segment level
hh_site = hh_site.groupby(['gvkey', 'siteid']).agg({'emple':'mean', 'reven':'mean'}).reset_index()
comp_seg = comp_seg.groupby(['gvkey', 'sid']).agg({'emps':'mean', 'seg_sale':'mean'}).reset_index()


# Create bins 
hh_site['emple_bin'] = pd.cut(hh_site['emple'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])
hh_site['reven_bin'] = pd.cut(hh_site['reven'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])

# Create bins for segment sales in comp_seg
comp_seg['seg_sale_bin'] = pd.cut(comp_seg['seg_sale'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])
comp_seg['emps_bin'] = pd.cut(comp_seg['emps'], bins=[0, 50, 100, 200, 500, 1000], labels=['0-50', '50-100', '100-200', '200-500', '500+'])
