'''Code to prepare segment level IT based on the matched dataset'''

import pandas as pd
import ast

match_file = r'data\hh_dataset\hh_comp_match_sic2d.csv'

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

# export 
expanded_df['site_id'] = expanded_df['site_id'].astype(str)
expanded_df = expanded_df.reset_index(drop=True)
expanded_df.to_stata(r'data\hh_dataset\seg_site_match.dta',version=117)
