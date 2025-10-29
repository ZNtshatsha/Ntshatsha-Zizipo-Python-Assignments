import os
from functools import partial
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "users_example.csv")
JSON_PATH = os.path.join(HERE, "users_example.json")
CLEANED_PATH = os.path.join(HERE, "users_cleaned.csv")

sample_data = [
    {'user_id': 1, 'name': 'Anele',     'age': '29',      'signup_date': '2021-05-14', 'last_login': '2025-09-15 08:34', 'country': 'South Africa', 'score': '85',  'premium': True,  'notes': 'Loves coding'},
    {'user_id': 2, 'name': 'Bongani',   'age': 'thirty',  'signup_date': '14/06/2022', 'last_login': '2025-10-05',       'country': '',              'score': '72.5','premium': False, 'notes': None},
    {'user_id': 3, 'name': 'Anele',     'age': '29',      'signup_date': '2021-05-14', 'last_login': '2024-12-01',       'country': None,            'score': '85',  'premium': True,  'notes': 'Duplicate entry'},
    {'user_id': 4, 'name': 'Chanel',    'age': '22',      'signup_date': '2020-11-02', 'last_login': '2025-10-01',       'country': 'Zimbabwe',      'score': 'N/A', 'premium': False, 'notes': ''},
    {'user_id': 5, 'name': 'Dumisani',  'age': '31',      'signup_date': '2023/02/28', 'last_login': '2025-09-20',       'country': 'South Africa', 'score': '90',  'premium': True,  'notes': 'VIP'},
    {'user_id': 6, 'name': 'Ester',     'age': '27',      'signup_date': 'not a date','last_login': '2025-09-10',       'country': 'Mozambique',    'score': '65',  'premium': False, 'notes': ' '},
    {'user_id': 7, 'name': 'Fikile',    'age': '35',      'signup_date': '01-03-2024', 'last_login': None,             'country': '',              'score': '88',  'premium': True,  'notes': None},
    {'user_id': 8, 'name': 'Gugulethu', 'age': '28',      'signup_date': '2024-07-07', 'last_login': '2025-10-06 13:00', 'country': 'South Africa', 'score': '82',  'premium': False, 'notes': 'New user'}
]

columns = ['user_id','name','age','signup_date','last_login','country','score','premium','notes']
df = pd.DataFrame(sample_data, columns=columns)

# Save example files
df.to_csv(CSV_PATH, index=False)
df.to_json(JSON_PATH, orient='records', date_format='iso')
print(f"Saved example files:\n - {CSV_PATH}\n - {JSON_PATH}\n")


top_scores = pd.Series([95, 90, 85], index=['user_5','user_1','user_3'])
print("Custom Series (top_scores):")
print(top_scores, "\n")

print("DataFrame columns:", df.columns.tolist(), "\n")

print("dtypes:\n", df.dtypes, "\n")
print("head():\n", df.head(), "\n")
print("tail():\n", df.tail(), "\n")
print("describe():\n", df.describe(include='all'), "\n")

print("Rows 2..4 by iloc:\n", df.iloc[2:5], "\n")
print("Columns ['name','age','score']:\n", df[['name','age','score']].head(), "\n")

flags = [True, False, True, False, True, False, True, False]
print("Rows selected by boolean flags:\n", df[flags], "\n")

df['age_numeric'] = pd.to_numeric(df['age'], errors='coerce')
print("Ages after to_numeric:\n", df[['name','age','age_numeric']], "\n")
age_filtered = df[(df['age_numeric'] >= 25) & (df['age_numeric'] <= 35)]
print("Filtered ages 25-35:\n", age_filtered[['user_id','name','age_numeric']], "\n")

print("Duplicated row mask:\n", df.duplicated(), "\n")
print("Unique name count:", df['name'].nunique(), "\n")
df_no_dup = df.drop_duplicates(subset=['name','signup_date'], keep='first')
print("After drop_duplicates(subset=['name','signup_date']):\n", df_no_dup, "\n")

df_conv = df.copy()
df_conv['age'] = pd.to_numeric(df_conv['age'], errors='coerce')
df_conv['score'] = pd.to_numeric(df_conv['score'], errors='coerce')
df_conv['signup_date'] = pd.to_datetime(df_conv['signup_date'], errors='coerce', dayfirst=True)
df_conv['last_login'] = pd.to_datetime(df_conv['last_login'], errors='coerce')
print("After conversions: dtypes:\n", df_conv.dtypes, "\n")
print(df_conv.head(), "\n")

def fill_country(x):
    if pd.isna(x) or str(x).strip() == '':
        return 'Unknown'
    return str(x).strip()

df_conv['country'] = df_conv['country'].apply(fill_country)
df_conv['notes'] = df_conv['notes'].apply(lambda x: 'No notes' if pd.isna(x) or str(x).strip() == '' else str(x).strip())
print("After filling 'country' and 'notes':\n", df_conv[['user_id','country','notes']], "\n")


def convert_types_and_report(dframe):
    d = dframe.copy()
    d['age'] = pd.to_numeric(d['age'], errors='coerce')
    d['score'] = pd.to_numeric(d['score'], errors='coerce')
    d['signup_date'] = pd.to_datetime(d['signup_date'], errors='coerce', dayfirst=True)
    d['last_login'] = pd.to_datetime(d['last_login'], errors='coerce')
    print("=== Inside pipeline: dtypes ===")
    print(d.dtypes)
    print("\n=== Inside pipeline: null counts ===")
    print(d.isnull().sum())
    return d

df_pipeline = df.pipe(convert_types_and_report)


def flag_high_scores(dframe, threshold=80):
    d = dframe.copy()
    d['score'] = pd.to_numeric(d['score'], errors='coerce')
    d['high_score'] = d['score'] > threshold
    return d

THRESHOLD = 80
df_final = df_pipeline.pipe(partial(flag_high_scores, threshold=THRESHOLD))

df_final.to_csv(CLEANED_PATH, index=False)
print(f"Saved cleaned CSV to: {CLEANED_PATH}\n")
print("Sample of final DataFrame:\n", df_final.head())
