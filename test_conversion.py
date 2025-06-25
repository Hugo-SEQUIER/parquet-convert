import pandas as pd

df = pd.read_parquet("test/train-00000-of-00008.parquet")

print(df.head())
