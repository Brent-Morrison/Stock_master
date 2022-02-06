import sys
import numpy as np
import pandas as pd

df = pd.DataFrame(np.random.randn(100, 4), columns=list('ABCD'))

fltr = int(sys.argv[1])

print(type(fltr))

df1 = df.head(fltr).copy()

df1.to_csv('C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\df1_test.csv')