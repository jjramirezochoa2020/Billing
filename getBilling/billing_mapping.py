# Get the VC service cost
def VC_cost(x, dict_sample):
    keyPast = 0
    for idx, key in enumerate(dict_sample):
        if x < key:
            return dict_sample[key][0]
        if x <= key and x > keyPast and idx >= 1:
            return dict_sample[key][0]
        if idx == len(dict_sample.keys()) - 1:
            return dict_sample[key][0]
        keyPast = key
        

# Get the E1 service cost
def E1_cost(x, dict_sample):
    keyPast = 0
    for idx, key in enumerate(dict_sample):
        if x < key:
            return dict_sample[key][1]
        if x <= key and x > keyPast and idx >= 1:
            return dict_sample[key][1]
        if idx == len(dict_sample.keys()) - 1:
            return dict_sample[key][1]
        keyPast = key
        

# Get the ET service cost
def ET_cost(x, dict_sample):
    x = float(x)
    keyPast = 0
    for idx, key in enumerate(dict_sample):
        if x < key:
            return dict_sample[key]
        if x <= key and x > keyPast and idx >= 1:
            return dict_sample[key]
        if idx == len(dict_sample.keys()) - 1:
            return dict_sample[key]
        keyPast = key

    
# Get the E1 services number    
def E1_quantity(x):
    return x[x['Tecn'] == 'E1']['Tecn'].count()


# Get the VC services number 
def VC_quantity(x):
    return x[x['Tecn'] == 'VC']['Tecn'].count()