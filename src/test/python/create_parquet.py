import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa

with open('src/test/resources/samples/example.json', 'r') as file:
    json = file.read()

df = pd.DataFrame({'col1': ['example.json'],
                   'col2': [json]})

table = pa.Table.from_pandas(df)

pq.write_table(table, 'example.parquet')
