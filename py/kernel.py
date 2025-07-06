import pandas as pd
import numpy as np
import os
import yaml
import warnings
warnings.filterwarnings('ignore')

download_path = 'C:/Users/Ana Branca/Downloads'
file_name =  'TXT250706135211.TAB'
output_path = '../output/'


# Read categories
with open('../dict/categories.yml', 'r') as stream:
    dict_categories = yaml.load(stream, Loader=yaml.FullLoader)

df = pd.read_csv( 
    os.path.join( download_path, file_name ) 
    , sep = '\t'
    , names = ['accountNumber', 'mutationcode', 'transactiondate', 'valuedate', 'startsaldo', 'endsaldo', 'amount', 'description']
)

print('- Reporting period:\n' + str(df.transactiondate.min()) + ' - ' + str(df.transactiondate.max()) )

flat_mapping = {}
for category, value in dict_categories.items():
    if isinstance(value, list):  # Flat list
        for keyword in value:
            flat_mapping[keyword.lower()] = (None, category, None)  # No subcategory and no share
    elif isinstance(value, dict):  # Nested subcategories
        for subcategory, details in value.items():
            share = details.get("share", None)  # Get the share from subcategory details
            for keyword in details["items"]:
                flat_mapping[keyword.lower()] = (subcategory, category, share)

# Function to find match and extract the share
def find_match(text):
    text_lower = text.lower()
    for keyword, (subcategory, category, share) in flat_mapping.items():
        if keyword in text_lower:
            return pd.Series([keyword, subcategory, category, share])
    return pd.Series([None, None, None, None])

# Apply the function to the DataFrame
df[['matched_keyword', 'subcategory', 'category', 'share']] = df['description'].apply(find_match)
df['amount'] = - df['amount'].apply(lambda x: float(x.replace(',', '.')))

# Show the result
df.head()

# coverage matched
print('- Coverage of matched: \n',
1 - sum( df['matched_keyword'].isna() ) / df.shape[0]
)

# 2. Analyse from the unmatch cases
df_unmatched = df[ df['matched_keyword'].isna() ]
df_unmatched_agg = df_unmatched.groupby('description')['accountNumber'].count().sort_values(ascending=False)
df_unmatched_agg

df_final = df[['matched_keyword', 'amount','transactiondate','subcategory', 'category','description', 'share']]
df_final['email_address'] = 'abrancap@gmail.com'
df_final['timestamp'] = pd.Timestamp.now()
df_final['description'] = np.where(df['subcategory'].notnull(), df['subcategory'], df['description'])
df_final['transactiondate'] = pd.to_datetime(df_final['transactiondate'], format='%Y%m%d').dt.strftime('%m/%d/%Y')

df_final = df_final[['timestamp', 'matched_keyword', 'email_address', 'amount','description','category', 'transactiondate', 'share']].copy()

df_final.to_excel(
    os.path.join( output_path, str(df.transactiondate.min()) + '_' + str(df.transactiondate.max()) + '_transactions.xlsx' ),
    index = False
)

print(' - File created!\n',
    os.path.join( output_path, str(df.transactiondate.min()) + '_' + str(df.transactiondate.max()) + '_transactions.xlsx' )
    )