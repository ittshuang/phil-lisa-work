'''Code to see the matching rate within HH datasets'''

import pandas as pd

hh = pd.read_csv(r'data\hh_dataset\hh_industry.csv') 
hh_year = hh[['gvkey','fyear']].drop_duplicates().reset_index(drop=True)

output_sic2d = r'data\hh_dataset\match\seg_site_match_sic2d.dta'
sic2d = pd.read_stata(output_sic2d) 
sic2d_year = sic2d[['fyear','gvkey','has_match']]
sic2d_year = sic2d_year.groupby(['fyear','gvkey']).agg({'has_match':'max'}).reset_index()
hh_year2 = hh_year.merge(sic2d_year, on=['fyear', 'gvkey'], how='left')

combined_df = hh_year2.groupby('fyear').agg(
     has_match_sum=('has_match', 'sum'),
     total_gvkey=('has_match', 'count')
 )

combined_df['has_match_proportion'] = combined_df['has_match_sum'] / combined_df['total_gvkey']

print(combined_df)