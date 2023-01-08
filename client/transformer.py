import numpy as np
import pandas as pd

df = pd.read_csv('../data/smalldata.csv')

df['json'] = df.to_json(orient='records', lines=True).splitlines()
df_json = df['json']

np.savetxt(r'../data/output.txt', df_json.values, fmt='%s')
