'''Code to prepare segment level IT based on the matched dataset'''

import pandas as pd
import ast

COMPSEG_FILE = r'data\firm_info\compustats_segment_00_21.dta'
# match_file = r'data\hh_dataset\hh_comp_match_sic2d.csv'
match_file_sic2d = r'data\hh_dataset\match\hh_comp_match_sic2d.csv'
match_file_sic1d = r'data\hh_dataset\match\hh_comp_match_sic1d.csv'

output_sic2d = r'data\hh_dataset\match\seg_site_match_sic2d.dta'
output_sic1d = r'data\hh_dataset\match\seg_site_match_sic1d.dta'


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

def map_industry(code):
    for code_range, abbr in industry_map.items():
        if code in code_range:
            return abbr
    return None

def filter_high_priority(df):
    min_priority = df.groupby(['gvkey', 'fyear'])['priority'].transform('min')
    return df[df['priority'] == min_priority]

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

def main(match_file, output):
    df = pd.read_csv(match_file)

    # drop seg_id na
    df = df.dropna(subset=['seg_id'])
    df['seg_id'] = df['seg_id'].apply(ast.literal_eval)

    # expand the database to segment id level
    expanded_rows = []
    for idx, row in df.iterrows():
        seg_ids = row['seg_id']
        for seg_id in seg_ids:
            new_row = row.copy()
            new_row['seg_id'] = seg_id
            expanded_rows.append(new_row)

    expanded_df = pd.DataFrame(expanded_rows)
    # create match indicator
    expanded_df['has_match'] = (~expanded_df.site_id.isna()).astype(int)

    # create site number
    expanded_df['site_id'] = expanded_df['site_id'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else x)
    expanded_df['site_count'] = expanded_df['site_id'].apply(lambda x: len(x) if isinstance(x, list) else 0)

    # some transformation
    expanded_df['site_id'] = expanded_df['site_id'].astype(str)
    expanded_df = expanded_df.reset_index(drop=True)
    if match_file == match_file_sic2d:
        expanded_df.drop(columns=['sic2d'], inplace=True)
    else:
        expanded_df.drop(columns=['sic1d'], inplace=True)
        
    expanded_df.rename(columns={'seg_sale':'seg_sale_sum'}, inplace=True)

    # Add segment level variable
    comp_seg=read_compustats_seg()
    comp_seg.rename(columns={'sid': 'seg_id'}, inplace=True)
    comp_seg.gvkey = comp_seg.gvkey.astype(int)

    expanded_df=expanded_df.merge(comp_seg, on=['fyear', 'gvkey', 'seg_id'], how='left').reset_index(drop=True)
    expanded_df=expanded_df.drop(columns=['priority'])

    # export 
    expanded_df.to_stata(output,version=117)
    
if __name__ == '__main__':
    main(match_file_sic2d, output_sic2d)
    main(match_file_sic1d, output_sic1d)