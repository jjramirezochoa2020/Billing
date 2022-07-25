import numpy as np
import calendar as cl
import billing_mapping as bm
import sql_db_functions as dbFuntions


def month_days(year, month): 
    num_days = cl.monthrange(year, month)[1]
    return num_days

def get_carrier_services(carrier, today, db_qa_conf):

    # Filtrar la ultima actualización de los servicios activos filtrados por la fecha "today", para recalcular la facturación del último mes
    sql_query = "SELECT * FROM servicesLogs WHERE Carrier = '{0}' AND Estado = 'Activo' AND (dateModificacionContrato <= '{1}')".format(carrier, today)
    Carrier_data = dbFuntions.execute_query(db_qa_conf, sql_query)
    return Carrier_data.sort_values(by='dateModificacionContrato', ascending=False).groupby('Consecutivo', as_index=False).first()


def get_E1_STM1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today):

    year = int(today[:4])
    month = int(today[5:7])
    days = cl.monthrange(year, month)[1]
    Carrier_data_filtered = Carrier_data[Carrier_data['CategoriaServicio'].isin(service_category)]

    # Generar un diccionario de la tabla de facturación del carrier
    cost_services_query = "SELECT Last_miles, STM1_price, E1_price FROM billing_information_{0} WHERE type = '{1}'".format(carrier, database_filter)
    cost_services = dbFuntions.execute_query(db_qa_conf, cost_services_query)
    for column in cost_services.columns:
        if str(cost_services.dtypes[column]) == 'object':
            cost_services[column] = cost_services[column].str.decode("utf-8")

    cost_services_dict = tuple(zip(list(cost_services['STM1_price']), list(cost_services['E1_price'])))
    cost_services_dict = dict(zip(list(cost_services['Last_miles']), cost_services_dict))

    # Calcular cantidad de E1 y de VC12
    E1 = bm.E1_quantity(Carrier_data_filtered)
    VC = bm.VC_quantity(Carrier_data_filtered)

    Carrier_data_filtered_E1 = Carrier_data_filtered[(Carrier_data_filtered['Tecn'] == 'E1') & (~Carrier_data_filtered['Capacidad'].str.contains('E1-T'))].reset_index(drop=True)
    Carrier_data_filtered_VC = Carrier_data_filtered[(Carrier_data_filtered['Tecn'] == 'VC') & (~Carrier_data_filtered['Capacidad'].str.contains('VC-T'))].reset_index(drop=True)

    # Escribir la facturación de los servicios activos en el dataframe billing
    if not Carrier_data_filtered_E1.empty:
        for i in range(Carrier_data_filtered_E1['Consecutivo'].count()):
            row = [today, Carrier_data_filtered_E1.loc[i, 'Consecutivo'], carrier, service_category[0], 'E1', '', bm.E1_cost(E1, cost_services_dict), bm.E1_cost(E1, cost_services_dict)/days]
            billing.loc[number_row] = row
            number_row += 1

    if not Carrier_data_filtered_VC.empty:
        for i in range(Carrier_data_filtered_VC['Consecutivo'].count()):
            row = [today, Carrier_data_filtered_VC.loc[i, 'Consecutivo'], carrier, service_category[0], 'VC', '', bm.VC_cost(VC, cost_services_dict), bm.VC_cost(VC, cost_services_dict)/days]
            billing.loc[number_row] = row
            number_row += 1
    
    return billing, number_row


def get_ET_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today):
    
    year = int(today[:4])
    month = int(today[5:7])
    days = cl.monthrange(year, month)[1]
    Carrier_data_filtered = Carrier_data[Carrier_data['CategoriaServicio'].isin(service_category)]

    # Generar un diccionario de la tabla de facturación del carrier
    cost_services_query = "SELECT Capacity, Price FROM billing_information_{0} WHERE type = '{1}'".format(carrier, database_filter)
    cost_services = dbFuntions.execute_query(db_qa_conf, cost_services_query)
    for column in cost_services.columns:
        if str(cost_services.dtypes[column]) == 'object':
            cost_services[column] = cost_services[column].str.decode("utf-8")

    cost_services_dict = dict(zip(list(cost_services['Capacity']), cost_services['Price']))

    Carrier_data_ET = Carrier_data_filtered[Carrier_data_filtered['Tecn'] == 'ET'].reset_index(drop=True)
    
    # Escribir la facturación de los servicios activos en el dataframe billing
    for i in range(Carrier_data_ET['Consecutivo'].count()):
        row = [today, Carrier_data_ET.loc[i, 'Consecutivo'], carrier, service_category[0], 'ET', Carrier_data_ET.loc[i, 'Capacidad'], bm.ET_cost(Carrier_data_ET.loc[i, 'Capacidad'], cost_services_dict), float(Carrier_data_ET.loc[i, 'Capacidad']) * bm.ET_cost(Carrier_data_ET.loc[i, 'Capacidad'], cost_services_dict)/days]
        billing.loc[number_row] = row
        number_row += 1

    return billing, number_row


def get_ET_calculation_TRM(Carrier_data, number_row, billing, TRM, service_category, carrier, database_filter, db_qa_conf, today):
    
    year = int(today[:4])
    month = int(today[5:7])
    days = cl.monthrange(year, month)[1]
    Carrier_data_filtered = Carrier_data[Carrier_data['CategoriaServicio'].isin(service_category)]

    # Generar un diccionario de la tabla de facturación del carrier
    cost_services_query = "SELECT Capacity, Price FROM billing_information_{0} WHERE type = '{1}'".format(carrier, database_filter)
    cost_services = dbFuntions.execute_query(db_qa_conf, cost_services_query)
    for column in cost_services.columns:
        if str(cost_services.dtypes[column]) == 'object':
            cost_services[column] = cost_services[column].str.decode("utf-8")

    cost_services_dict = dict(zip(list(cost_services['Capacity']), cost_services['Price']))

    Carrier_data_ET = Carrier_data_filtered[Carrier_data_filtered['Tecn'] == 'ET'].reset_index(drop=True)
    
    # Escribir la facturación de los servicios activos en el dataframe billing
    for i in range(Carrier_data_ET['Consecutivo'].count()):
        row = [today, Carrier_data_ET.loc[i, 'Consecutivo'], carrier, service_category[0], 'ET', Carrier_data_ET.loc[i, 'Capacidad'],  bm.ET_cost(Carrier_data_ET.loc[i, 'Capacidad']*TRM, cost_services_dict), float(Carrier_data_ET.loc[i, 'Capacidad']) * bm.ET_cost(Carrier_data_ET.loc[i, 'Capacidad'], cost_services_dict)*TRM/days]
        billing.loc[number_row] = row
        number_row += 1
        print(row)

    return billing, number_row


def get_aggregation_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today):

    year = int(today[:4])
    month = int(today[5:7])
    days = cl.monthrange(year, month)[1]
    Carrier_data_filtered = Carrier_data[Carrier_data['CategoriaServicio'].isin(service_category)]
    
    # Generar un diccionario de la tabla de facturación del carrier
    cost_services_query = "SELECT Capacity, Price FROM billing_information_{0} WHERE type = '{1}'".format(carrier, database_filter)
    cost_services = dbFuntions.execute_query(db_qa_conf, cost_services_query)
    for column in cost_services.columns:
        if str(cost_services.dtypes[column]) == 'object':
            cost_services[column] = cost_services[column].str.decode("utf-8")

    cost_services_dict = dict(zip(list(cost_services['Capacity']), cost_services['Price']))

    Carrier_data_aggregation = Carrier_data_filtered[Carrier_data_filtered['Tecn'] == 'ET'].reset_index(drop=True)
    total_capacity = Carrier_data_aggregation['Capacidad'].astype(float).sum()

    # Escribir la facturación de los servicios activos en el dataframe billing
    for i in range(Carrier_data_aggregation['Consecutivo'].count()):
        row = [today, Carrier_data_aggregation.loc[i, 'Consecutivo'], carrier, service_category[0], 'ET', Carrier_data_aggregation.loc[i, 'Capacidad'], bm.ET_cost(total_capacity, cost_services_dict), float(Carrier_data_aggregation.loc[i, 'Capacidad']) * bm.ET_cost(total_capacity, cost_services_dict)/days]
        billing.loc[number_row] = row
        number_row += 1

    return billing, number_row


def get_special_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today):

    year = int(today[:4])
    month = int(today[5:7])
    days = cl.monthrange(year, month)[1]
    Carrier_data_filtered = Carrier_data[Carrier_data['CategoriaServicio'].isin(service_category)].reset_index(drop=True)

    special_services_query = "SELECT `Description`, Price FROM billing_information_{0} WHERE type = '{1}'".format(carrier, database_filter)
    special_services_df = dbFuntions.execute_query(db_qa_conf, special_services_query)
    special_services = {a:b for a,b in zip(special_services_df['Description'].str.normalize("NFKC"), special_services_df['Price'])}
    print(special_services)

    for i in range(Carrier_data_filtered['Consecutivo'].count()):
        print(Carrier_data_filtered.loc[i, 'TipoTrafico'])
        row = [today, Carrier_data_filtered.loc[i, 'Consecutivo'], carrier, service_category[0], service_category[0], '', '', special_services[Carrier_data_filtered.loc[i, 'TipoTrafico']]/days]
        billing.loc[number_row] = row
        number_row += 1

    return billing, number_row


def get_special_E1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today):
    
    year = int(today[:4])
    month = int(today[5:7])
    days = cl.monthrange(year, month)[1]
    Carrier_data_filtered = Carrier_data[Carrier_data['Capacidad'].str.contains(service_category[0])].reset_index(drop=True)

    special_services_query = "SELECT `Description`, Price FROM billing_information_{0} WHERE type = '{1}'".format(carrier, database_filter)
    special_services_df = dbFuntions.execute_query(db_qa_conf, special_services_query)
    special_services = {a:b for a,b in zip(special_services_df['Description'].str.normalize("NFKC"), special_services_df['Price'])}
    # print(special_services)
    Special_rate_list = list(Carrier_data_filtered['Capacidad'].unique())
    # print('TEST: ', Special_rate_list)
    for special_rate in Special_rate_list:
        Carrier_data_filtered = Carrier_data_filtered[Carrier_data_filtered['Capacidad'] == special_rate]
        for i in range(Carrier_data_filtered['Consecutivo'].count()):
            row = [today, Carrier_data_filtered.loc[i, 'Consecutivo'], carrier, service_category[0], service_category[0], '', special_services[special_rate], special_services[special_rate]/days]
            billing.loc[number_row] = row
            number_row += 1

    return billing, number_row


def get_GTD_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today):
    
    year = int(today[:4])
    month = int(today[5:7])
    days = cl.monthrange(year, month)[1]
    Carrier_data_filtered = Carrier_data[Carrier_data['CategoriaServicio'].isin(service_category)]

    # Generar un diccionario de la tabla de facturación del carrier
    cost_services_query = "SELECT Capacity, Price FROM billing_information_{0} WHERE type = '{1}'".format(carrier, database_filter)
    cost_services = dbFuntions.execute_query(db_qa_conf, cost_services_query)
    for column in cost_services.columns:
        if str(cost_services.dtypes[column]) == 'object':
            cost_services[column] = cost_services[column].str.decode("utf-8")

    cost_services_dict = dict(zip(list(cost_services['Capacity']), cost_services['Price']))

    Carrier_data_ET = Carrier_data_filtered[Carrier_data_filtered['Tecn'] == 'ET'].reset_index(drop=True)
    total_capacity = sum(Carrier_data_ET['Capacidad'].astype(int))*2
    cost = bm.ET_cost(total_capacity, cost_services_dict)
    
    # Escribir la facturación de los servicios activos en el dataframe billing
    for i in range(Carrier_data_ET['Consecutivo'].count()):
        print('TEST: ', float(Carrier_data_ET.loc[i, 'Capacidad'])*cost/30)
        row = [today, Carrier_data_ET.loc[i, 'Consecutivo'], carrier, service_category[0], 'ET', Carrier_data_ET.loc[i, 'Capacidad'], cost, float(Carrier_data_ET.loc[i, 'Capacidad']) * cost]
        billing.loc[number_row] = row
        number_row += 1

    return billing, number_row




   