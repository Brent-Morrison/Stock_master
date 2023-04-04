import numpy as np
import pandas as pd
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score


def theil_sen_py(df, iter=20):
    """Theil Sen regression, accepts dataframe, iterates for average prediction
    
    Keyord arguments:
    df -- dataframe, structured as follows:
    - first column:         date
    - second column:        sector
    - third column:         labels/ticker
    - fourth column:        y values
    - remaining columns:    x values

    iter -- number of iteration for regression model
    """
    c = len(df.columns)
    r = len(df)
    d = df.iloc[:, 0:1]
    s = df.iloc[:, 1:2]
    t = df.iloc[:, 2:3]
    y = df.iloc[:, 3:4]
    x = df.iloc[:, 4:c]
    yhat_int = np.zeros(r)
    for i in range(1,iter+1,1):
        tsr = linear_model.TheilSenRegressor(random_state=i, max_iter=250)
        model = tsr.fit(x, np.ravel(y))
        yhat_mdl = model.predict(x)
        yhat_int = yhat_int+yhat_mdl
        yhat = yhat_int/i
        rsdl = np.ravel(y)-yhat
    return pd.DataFrame({
        'date_stamp': np.ravel(d)
        ,'sector': np.ravel(s)
        ,'ticker': np.ravel(t)
        ,'prediction': yhat
        ,'residual': rsdl
        })