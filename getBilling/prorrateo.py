import numpy as np
import pandas as pd

# df: dataframe sin ajuste de cálculos

df = pd.DataFrame([['ID001', 'Enero', 96774.2], ['ID001', 'Enero', 96774.2], ['ID001', 'Enero', 96774.2], 
                   ['ID001', 'Enero', 96774.2], ['ID001', 'Enero', 96774.2], ['ID001', 'Enero', 96774.2], 
                   ['ID001', 'Enero', 96774.2], ['ID001', 'Enero', 96774.2], ['ID001', 'Enero', 96774.2], 
                   ['ID001', 'Enero', 96774.2], ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], 
                   ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], 
                   ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3],
                   ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], 
                   ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], 
                   ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], 
                   ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3], ['ID001', 'Enero', 161290.3],
                   ['ID001', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], 
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3],
                   ['ID002', 'Enero', 161290.3], ['ID002', 'Enero', 161290.3]], columns=['ID', 'month', 'value'])

df_recalculated = pd.DataFrame(columns=['ID', 'month', 'value'])
df_check = df


def extract_month(x):

    return str(x)[5:7]
    

df_check = df.groupby(['ID', 'month']).agg({'value': pd.Series.nunique})
df_check

df_prorrateo = df_check.loc[df_check['value'] > 1].reset_index()
list_prorrateo = dict(zip(df_prorrateo['ID'], df_prorrateo['value']))
print(list_prorrateo)

tarifa_ant = 0
parcial_num = 1


for service in list_prorrateo.keys():
    print(service)

