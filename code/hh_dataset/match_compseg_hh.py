'''Code to match the compustata segment data with HH Datasets'''

import pandas as pd 
import os
import numpy as np
from tqdm import tqdm

# INPUTS
COMPSEG_FILE = r'data\firm_info\compustats_segment_00_21.dta'
HH_FILE = r'data\hh_dataset\publicfirm_site.dta'
HH_industry = r'data\hh_dataset\hh_industry.csv'

#HH_FIL2E = r"data\hh_dataset\2001_2009_site_clean_data.dta"

# OUTPUTS - store it to the new folder
OUTPUT_SIC2d = r'data\hh_dataset\match\hh_comp_match_sic2d.csv'
OUTPUT_SIC1d = r'data\hh_dataset\match\hh_comp_match_sic1d.csv'

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
        ['gvkey','fyear','total_sales','sid','stype','snms','SICS1','NAICSS1',
         'emps','seg_sale','seg_ops', 'seg_oibdps', 'seg_rds', 'seg_capxs',
       'seg_ias', 'seg_ppe']
        ].sort_values(['gvkey','fyear']).reset_index(drop=True)
    
    # Only keep year 2001 to 2009 as the HH data is from 2001 to 2009
    comp_seg = comp_seg[(comp_seg['fyear']<=2008) & (comp_seg['fyear']>=2001)].reset_index(drop=True)
    
    # only keep one stype based on the priority
    comp_seg['priority'] = comp_seg['stype'].map(priority)
    comp_seg = filter_high_priority(comp_seg)
    comp_seg['sic2d'] = comp_seg['SICS1'].astype(str).str[:2].astype(int) 
    comp_seg['sic1d'] = comp_seg['SICS1'].astype(str).str[:1].astype(int) 
    comp_seg['SICGRP'] = comp_seg['sic2d'].apply(map_industry)
    return comp_seg

def return_hh_sic():
    raw_list = []
    for year in tqdm(range(2001,2009)):
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
        ['gvkey','year','corpid','siteid','emple','reven','avg_reven',
         'internet','intranet', 'intranet_MS', 'totpc', 'ahq', 'distance_to_hq', 'avg_pc',] # from documentation: reven is also in millions
        ].sort_values(['gvkey','year']).reset_index(drop=True)
    # No industry for this processed industry data - add industry data from the raw data
    # add sic code 
    hh_sic = return_hh_sic()
    hh_site.rename(columns={'year':'fyear'}, inplace=True)
    # merge with hh_site sic code
    hh_site = hh_site.merge(hh_sic, on=['siteid','fyear'], how='left' )
    hh_site['sic2d'] = hh_site['siccode'].astype(str).str[:2]
    hh_site['sic1d'] = hh_site['siccode'].astype(str).str[:1]
    
    hh_site['sic2d'] = pd.to_numeric(hh_site['sic2d'], errors='coerce')
    hh_site = hh_site.dropna(subset=['sic2d'])
    hh_site['sic2d'] = hh_site['sic2d'].astype(int)
    
    hh_site['sic1d'] = pd.to_numeric(hh_site['sic1d'], errors='coerce')
    hh_site = hh_site.dropna(subset=['sic1d'])
    hh_site['sic1d'] = hh_site['sic1d'].astype(int)
    #hh_site.to_csv(HH_industry, index=False)
    return hh_site

def create_revenue_bin(df, col):
    '''
    Function to create revenue bins for sales columns in compustat segment
    and HH site data
    '''
    if df[col].min() >= 0:
        return pd.cut(df[col], bins=[df[col].min(), 50, 100, 200, 500,1000, df[col].max()], labels=['0-50', '50-100', '100-200', '200-500', '500-1000','1000+'])
    else:
        return pd.cut(df[col], bins=[df[col].min(), 0, 50, 100, 200, 500,1000, df[col].max()], labels=['< 0','0-50', '50-100', '100-200', '200-500', '500-1000','1000+'])


def determine_match_type(row):
    '''Return the matching type'''
    
    if type(row['seg_id']) == float or type(row['site_id']) == float:
        return np.nan
    else:
        seg_len = len(row['seg_id'])
        site_len = len(row['site_id'])
        
        if seg_len == 1 and site_len == 1:
            return 'one-to-one'
        elif seg_len == 1 and site_len > 1:
            return 'one-to-many'
        elif seg_len > 1 and site_len == 1:
            return 'many-to-one'
        elif seg_len > 1 and site_len > 1:
            return 'many-to-many'

def main(match_by,
         output):
    # read input data
    comp_seg = read_compustats_seg()
    hh_site = read_hh_site()

    # group match_by for compustats data
    comp_seg_ind_group = comp_seg.groupby(['fyear','gvkey',match_by]).agg(
        {"sid":list, 
         'seg_sale':'sum',
         'emps':'sum'}).reset_index()
    
    # create revenue bin
    comp_seg_ind_group['seg_sale_bin'] = create_revenue_bin(comp_seg_ind_group, 'seg_sale').astype(str)
    
    # group sic2d and output the sum of the IT variables
    hh_site_group = hh_site.groupby(
        ['fyear','gvkey',match_by]).agg({
            "siteid":list, 
            'reven':'sum',
            'emple':'sum',
            'internet':'max', # the three are indicator variable so get the max
            'intranet':'max', 
            'intranet_MS':'max', 
            'totpc':'sum', # get the sum for total PC
            'ahq': 'max', # if  the matched sites includes HQ
            }).reset_index()
    
    hh_site_group['site_sale_bin'] = create_revenue_bin(hh_site_group, 'reven').astype(str)
    
    # change col name
    hh_columns = [
        'fyear',
        'gvkey',
        match_by,
        'siteid',
        'reven_sum',
        'emple_sum',
        'internet',
        'intranet',
        'intranet_MS',
        'totpc_sum',
        'include_hq',
        'site_sale_bin']
    hh_site_group.columns = hh_columns

    result = comp_seg_ind_group.merge(hh_site_group, on=['fyear','gvkey',match_by],how='outer').sort_values(['gvkey','fyear',match_by])
    
    # change name of columns
    cols = ['fyear',
            'gvkey',
            match_by,
            'sid',
            'seg_sale',
            'seg_sale_bin',
            'emps',
            'siteid',
            'reven_sum',
            'emple_sum',
            'internet',
            'intranet',
            'intranet_MS',
            'totpc_sum',
            'include_hq',
            'site_sale_bin']
    
    result = result[cols]
    # rename columns
    result = result.rename(columns={
        'sid':'seg_id',
        'emps':'seg_emps',
        'siteid': 'site_id',
        'reven_sum':'site_reven_sum',
        'emple_sum':'site_emple_sum',
    })

    # create revenue_bin_match indicator 
    result['revenue_bin_match'] = (result['seg_sale_bin']==result['site_sale_bin']).astype(int)
    
    # create match_type column
    result['match_type'] = result.apply(determine_match_type, axis=1)
    
    result.to_csv(output, index=False)

if __name__ == '__main__':
    main(match_by = 'sic2d', output=OUTPUT_SIC2d)
    main(match_by = 'sic1d', output=OUTPUT_SIC1d)

