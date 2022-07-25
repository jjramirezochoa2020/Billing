import unicodedata
import numpy as np
import pandas as pd
import mysql.connector
# import CW_methods as CW
# import ETB_methods as ETB
# import UFINET_methods as UF
# import INTERNEXA_methods as IT
from datetime import datetime
from datetime import timedelta
import sql_db_functions as dbFuntions


numberRow = 0
carrierList = ['CW', 'UF', 'EB']
table = 'billing_carriers'
db_qa_conf = {'user':'collector_qa','password':'WOM1234','host':'10.40.111.103','database':'BILLING'}


for carrier in carrierList:

    for day in range(1):

        today = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d')

        if carrier == 'CW':
    
            billing = pd.DataFrame([['','','','','','']], columns=['date_time', 'ID_service', 'carrier', 'service_category', 'type_service', 'cost'])
            sql_query = "SELECT * FROM servicesLogs WHERE Carrier = '{0}' AND Estado = 'Activo' AND (dateModificacionContrato <= '{1}')".format(carrier, today)
            Carrier_data = dbFuntions.execute_query(db_qa_conf, sql_query)
            Carrier_data = Carrier_data.sort_values(by='dateModificacionContrato', ascending=False).groupby('Consecutivo', as_index=False).first()
            print(Carrier_data['Consecutivo'].count())


            # Cálculo Interconexión Nacional
            Carrier_data_national = Carrier_data[Carrier_data['CategoriaServicio'] == 'Interconexión Nacional']

            cost_services_national_interconnection_query = "SELECT Last_miles AS quantity, STM1_price AS VC_price, E1_price FROM billing_information_CW WHERE type = 'national E1'"
            cost_services_national_interconnection = dbFuntions.execute_query(db_qa_conf, cost_services_national_interconnection_query)
            for column in cost_services_national_interconnection.columns:
                if str(cost_services_national_interconnection.dtypes[column]) == 'object':
                    cost_services_national_interconnection[column] = cost_services_national_interconnection[column].str.decode("utf-8")

            cost_services_national_interconnection_dict = tuple(zip(list(cost_services_national_interconnection['VC_price']), list(cost_services_national_interconnection['E1_price'])))
            cost_services_national_interconnection_dict = dict(zip(list(cost_services_national_interconnection['quantity']), cost_services_national_interconnection_dict))

            E1 = CW.E1_quantity(Carrier_data_national)
            VC = CW.VC_quantity(Carrier_data_national)

            Carrier_data_national_E1 = Carrier_data_national[Carrier_data_national['Tecn'] == 'E1'].reset_index(drop=True)
            Carrier_data_national_VC = Carrier_data_national[Carrier_data_national['Tecn'] == 'VC'].reset_index(drop=True)

            if not Carrier_data_national_E1.empty():
                for i in range(Carrier_data_national_E1['Consecutivo'].count()):
                    row = [today, Carrier_data_national_E1.loc[i, 'Consecutivo'], carrier, 'Interconexion nacional', 'E1', CW.E1_cost(E1, cost_services_national_interconnection_dict)/30]
                    billing.loc[numberRow] = row
                    numberRow += 1

                for i in range(Carrier_data_national_VC['Consecutivo'].count()):
                    row = [today, Carrier_data_national_VC.loc[i, 'Consecutivo'], carrier, 'Interconexion nacional', 'VC', CW.VC_cost(VC, cost_services_national_interconnection_dict)/30]
                    billing.loc[numberRow] = row
                    numberRow += 1


            # Cálculo Interconexión Metropolitana
            Carrier_data_metropolitan = Carrier_data[(Carrier_data['CategoriaServicio'] == 'Enlaces Agregación Metropolitana') | (Carrier_data['CategoriaServicio'] == 'Interconexión Metropolitana')]

            Carrier_data_metropolitan_E1 = Carrier_data_metropolitan[Carrier_data_metropolitan['Tecn'] == 'E1'].reset_index(drop=True)
            Carrier_data_metropolitan_VC = Carrier_data_metropolitan[Carrier_data_metropolitan['Tecn'] == 'VC'].reset_index(drop=True)

            cost_services_metropolitan_interconnection_query = "SELECT Last_miles AS quantity, STM1_price AS VC_price, E1_price FROM billing_information_CW WHERE type = 'metropolitan E1'"
            cost_services_metropolitan_interconnection = dbFuntions.execute_query(db_qa_conf, cost_services_metropolitan_interconnection_query)
            for column in cost_services_metropolitan_interconnection.columns:
                if str(cost_services_metropolitan_interconnection.dtypes[column]) == 'object':
                    cost_services_metropolitan_interconnection[column] = cost_services_metropolitan_interconnection[column].str.decode("utf-8")

            cost_services_metropolitan_interconnection_dict = tuple(zip(list(cost_services_metropolitan_interconnection['VC_price']), list(cost_services_metropolitan_interconnection['E1_price'])))
            cost_services_metropolitan_interconnection_dict = dict(zip(list(cost_services_metropolitan_interconnection['quantity']), cost_services_metropolitan_interconnection_dict))

            E1 = CW.E1_quantity(Carrier_data_metropolitan)
            VC = CW.VC_quantity(Carrier_data_metropolitan)

            for i in range(Carrier_data_metropolitan_VC['Consecutivo'].count()):
                row = [today, Carrier_data_metropolitan_VC.loc[i, 'Consecutivo'], carrier, 'Interconexion metropolitana', 'VC', CW.VC_cost(VC, cost_services_metropolitan_interconnection_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1


            # Cálculo enlaces ET interconexión metropolitana
            cost_services_metropolitan_interconnection_query = "SELECT Capacity AS `range`, Price AS price FROM billing_information_CW WHERE type = 'Point to Point metropolitan aggregation'"
            cost_services_metropolitan_interconnection = dbFuntions.execute_query(db_qa_conf, cost_services_metropolitan_interconnection_query)
            for column in cost_services_metropolitan_interconnection.columns:
                if str(cost_services_metropolitan_interconnection.dtypes[column]) == 'object':
                    cost_services_metropolitan_interconnection[column] = cost_services_metropolitan_interconnection[column].str.decode("utf-8")

            cost_services_metropolitan_interconnection_dict = dict(zip(list(cost_services_metropolitan_interconnection['range']), cost_services_metropolitan_interconnection['price']))

            Carrier_data_metropolitan_ET = Carrier_data_metropolitan[(Carrier_data_metropolitan['Tecn'] == 'ET') & (Carrier_data_metropolitan['CategoriaServicio'] == 'Interconexión Metropolitana')].reset_index(drop=True)

            for i in range(Carrier_data_metropolitan_ET['Consecutivo'].count()):
                row = [today, Carrier_data_metropolitan_ET.loc[i, 'Consecutivo'], carrier, 'Interconexion metropolitana', 'ET', int(Carrier_data_metropolitan_ET.loc[i, 'Capacidad']) * CW.ET_cost(Carrier_data_metropolitan_ET.loc[i, 'Capacidad'], cost_services_metropolitan_interconnection_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1


            # Agregaciones metropolitanas
            cost_services_metropolitan_aggregation_query = "SELECT Capacity AS `range`, Price AS price FROM billing_information_CW WHERE type = 'Metropolitan Aggregation'"
            cost_services_metropolitan_aggregation = dbFuntions.execute_query(db_qa_conf, cost_services_metropolitan_aggregation_query)
            for column in cost_services_metropolitan_aggregation.columns:
                if str(cost_services_metropolitan_aggregation.dtypes[column]) == 'object':
                    cost_services_metropolitan_aggregation[column] = cost_services_metropolitan_aggregation[column].str.decode("utf-8")

            cost_services_metropolitan_aggregation_dict = dict(zip(list(cost_services_metropolitan_aggregation['range']), cost_services_metropolitan_aggregation['price']))

            Carrier_data_metropolitan_ET = Carrier_data_metropolitan[(Carrier_data_metropolitan['Tecn'] == 'ET') & (Carrier_data_metropolitan['CategoriaServicio'] == 'Enlaces Agregación Metropolitana')].reset_index(drop=True)

            for i in range(Carrier_data_metropolitan_ET['Consecutivo'].count()):
                row = [today, Carrier_data_metropolitan_ET.loc[i, 'Consecutivo'], carrier, 'Enlaces Agregación metropolitana', 'ET', int(Carrier_data_metropolitan_ET.loc[i, 'Capacidad']) * CW.ET_cost(Carrier_data_metropolitan_ET.loc[i, 'Capacidad'], cost_services_metropolitan_aggregation_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1


            # Cálculo Servicios IP Transit
            IP_transit_services_query = "SELECT `Description`, Price FROM billing_information_CW WHERE type = 'IP Transit service'"
            IP_transit_services_df = dbFuntions.execute_query(db_qa_conf, IP_transit_services_query)
            IP_transit_services = {a:b for a,b in zip(IP_transit_services_df['Description'].str.normalize("NFKC"), IP_transit_services_df['Price'])}

            # {
            #     'Canal de Internet Corporativo/iDEN CWC': 3428000.0,
            #     'Servicios de ISP WOM Bogotá - C&W': 13712000.0,
            #     'Servicios de ISP Bogotá RSO ODATA - C&W': 4600000.0,
            #     'C&W INTERNET 100 MBPS SERVICE IP': 3428000.0,
            #     'C&W INTERNET 5000 MBPS SERVICE IP': 30852000.0,
            #     'Servicios de ISP WOM Medellín - C&W': 20400000.0,
            #     'Canal TI Equinix': 3428000.0
            # }

            Carrier_data_IP_transit = Carrier_data[Carrier_data['CategoriaServicio'] == 'IP Transit'].reset_index(drop=True)

            for i in range(Carrier_data_IP_transit['Consecutivo'].count()):
                row = [today, Carrier_data_IP_transit.loc[i, 'Consecutivo'], carrier, 'IP Transit', 'IP Transit', IP_transit_services[Carrier_data_IP_transit.loc[i, 'TipoTrafico']]/30]
                billing.loc[numberRow] = row
                numberRow += 1

            billing.to_csv('temp_table.csv', index=False, encoding='iso-8859-1')
            result = dbFuntions.append_csv_data(db_qa_conf, 'temp_table.csv', table)
            print (result)

        
        if carrier == 'UF':
    
            billing = pd.DataFrame([['','','','','','']], columns=['date_time', 'ID_service', 'carrier', 'service_category', 'type_service', 'cost'])
            sql_query = "SELECT * FROM servicesLogs WHERE Carrier = '{0}' AND Estado = 'Activo' AND (dateModificacionContrato <= '{1}')".format(carrier, today)
            Carrier_data = dbFuntions.execute_query(db_qa_conf, sql_query)
            Carrier_data = Carrier_data.sort_values(by='dateModificacionContrato', ascending=False).groupby('Consecutivo', as_index=False).first()
            print(Carrier_data['Consecutivo'].count())

            # Cálculo Agregaciones metropolitanas
            Carrier_data_metropolitan = Carrier_data[Carrier_data['CategoriaServicio'] == "Enlaces Agregación Metropolitana"].reset_index(drop=True)
            total_capacity = Carrier_data_metropolitan['Capacidad'].astype(float).sum()

            cost_services_metropolitan_aggregation_query = "SELECT * FROM metropolitan_capacity_UFINET"
            cost_services_metropolitan_aggregation = dbFuntions.execute_query(db_qa_conf, cost_services_metropolitan_aggregation_query)
            cost_services_metropolitan_aggregation_dict = dict(zip(list(cost_services_metropolitan_aggregation['range']), cost_services_metropolitan_aggregation['price']))

            cost_per_MB = UF.Capacity_cost_per_MB(total_capacity, cost_services_metropolitan_aggregation_dict)
            for i in range(Carrier_data_metropolitan['Consecutivo'].count()):
                row = [today, Carrier_data_metropolitan.loc[i, 'Consecutivo'], carrier, 'Enlaces Agregación metropolitana', 'ET', int(Carrier_data_metropolitan.loc[i, 'Capacidad']) * cost_per_MB / 30]
                billing.loc[numberRow] = row
                numberRow += 1

            # Cálculo Agregaciones nacionales y especiales
            Carrier_data_national = Carrier_data[(Carrier_data['CategoriaServicio'] == "Enlaces Agregación Nacional") | (Carrier_data['CategoriaServicio'] == "Enlaces Especiales")].reset_index(drop=True)
            total_capacity = Carrier_data_metropolitan['Capacidad'].astype(float).sum()

            cost_services_national_aggregation_query = "SELECT * FROM national_capacity_UFINET"
            cost_services_national_aggregation = dbFuntions.execute_query(db_qa_conf, cost_services_national_aggregation_query)
            cost_services_national_aggregation_dict = dict(zip(list(cost_services_national_aggregation['range']), cost_services_national_aggregation['price']))

            cost_per_MB = UF.Capacity_cost_per_MB(total_capacity, cost_services_national_aggregation_dict) / 1000
            for i in range(Carrier_data_national['Consecutivo'].count()):
                row = [today, Carrier_data_national.loc[i, 'Consecutivo'], carrier, 'Enlaces Agregación nacional ó especial', 'ET', int(Carrier_data_national.loc[i, 'Capacidad']) * cost_per_MB / 30]
                billing.loc[numberRow] = row
                numberRow += 1

            # billing.to_csv('temp_table.csv', index=False, encoding='iso-8859-1')
            # result = dbFuntions.append_csv_data(db_qa_conf, 'temp_table.csv', table)
            # print (result)

        
        if carrier == 'EB':
        
            billing = pd.DataFrame([['','','','','','']], columns=['date_time', 'ID_service', 'carrier', 'service_category', 'type_service', 'cost'])
            sql_query = "SELECT * FROM servicesLogs WHERE Carrier = '{0}' AND Estado = 'Activo' AND (dateModificacionContrato <= '{1}')".format(carrier, today)
            Carrier_data = dbFuntions.execute_query(db_qa_conf, sql_query)
            Carrier_data = Carrier_data.sort_values(by='dateModificacionContrato', ascending=False).groupby('Consecutivo', as_index=False).first()
            print(Carrier_data['Consecutivo'].count())


            # Cálculo Interconexión Nacional
            Carrier_data_national = Carrier_data[Carrier_data['CategoriaServicio'] == 'Interconexión Nacional']

            cost_services_national_interconnection_query = "SELECT Last_miles AS quantity, STM1_price AS VC_price, E1_price FROM billing_information_ETB WHERE type = 'national E1'"
            cost_services_national_interconnection = dbFuntions.execute_query(db_qa_conf, cost_services_national_interconnection_query)
            for column in cost_services_national_interconnection.columns:
                if str(cost_services_national_interconnection.dtypes[column]) == 'object':
                    cost_services_national_interconnection[column] = cost_services_national_interconnection[column].str.decode("utf-8")

            cost_services_national_interconnection_dict = tuple(zip(list(cost_services_national_interconnection['VC_price']), list(cost_services_national_interconnection['E1_price'])))
            cost_services_national_interconnection_dict = dict(zip(list(cost_services_national_interconnection['quantity']), cost_services_national_interconnection_dict))

            E1 = CW.E1_quantity(Carrier_data_national)
            VC = CW.VC_quantity(Carrier_data_national)

            Carrier_data_national_E1 = Carrier_data_national[Carrier_data_national['Tecn'] == 'E1'].reset_index(drop=True)
            Carrier_data_national_VC = Carrier_data_national[Carrier_data_national['Tecn'] == 'VC'].reset_index(drop=True)

            for i in range(Carrier_data_national_E1['Consecutivo'].count()):
                row = [today, Carrier_data_national_E1.loc[i, 'Consecutivo'], carrier, 'Interconexion nacional', 'E1', ETB.E1_cost(E1, cost_services_national_interconnection_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1

            for i in range(Carrier_data_national_VC['Consecutivo'].count()):
                row = [today, Carrier_data_national_VC.loc[i, 'Consecutivo'], carrier, 'Interconexion nacional', 'VC', ETB.VC_cost(VC, cost_services_national_interconnection_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1


            # Cálculo Interconexión Metropolitana
            Carrier_data_metropolitan = Carrier_data[(Carrier_data['CategoriaServicio'] == 'Enlaces Agregación Metropolitana') | (Carrier_data['CategoriaServicio'] == 'Interconexión Metropolitana')]

            Carrier_data_metropolitan_E1 = Carrier_data_metropolitan[Carrier_data_metropolitan['Tecn'] == 'E1'].reset_index(drop=True)
            Carrier_data_metropolitan_VC = Carrier_data_metropolitan[Carrier_data_metropolitan['Tecn'] == 'VC'].reset_index(drop=True)

            cost_services_metropolitan_interconnection_query = "SELECT Last_miles AS quantity, STM1_price AS VC_price, E1_price FROM billing_information_ETB WHERE type = 'metropolitan E1'"
            cost_services_metropolitan_interconnection = dbFuntions.execute_query(db_qa_conf, cost_services_metropolitan_interconnection_query)
            for column in cost_services_metropolitan_interconnection.columns:
                if str(cost_services_metropolitan_interconnection.dtypes[column]) == 'object':
                    cost_services_metropolitan_interconnection[column] = cost_services_metropolitan_interconnection[column].str.decode("utf-8")

            cost_services_metropolitan_interconnection_dict = tuple(zip(list(cost_services_metropolitan_interconnection['VC_price']), list(cost_services_metropolitan_interconnection['E1_price'])))
            cost_services_metropolitan_interconnection_dict = dict(zip(list(cost_services_metropolitan_interconnection['quantity']), cost_services_metropolitan_interconnection_dict))

            E1 = CW.E1_quantity(Carrier_data_metropolitan)
            VC = CW.VC_quantity(Carrier_data_metropolitan)

            for i in range(Carrier_data_metropolitan_VC['Consecutivo'].count()):
                row = [today, Carrier_data_metropolitan_VC.loc[i, 'Consecutivo'], carrier, 'Interconexion metropolitana', 'VC', ETB.VC_cost(VC, cost_services_metropolitan_interconnection_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1


            # Cálculo enlaces ET interconexión metropolitana
            cost_services_metropolitan_interconnection_query = "SELECT Capacity AS `range`, Price AS price FROM billing_information_ETB WHERE type = 'Point to Point metropolitan aggregation'"
            cost_services_metropolitan_interconnection = dbFuntions.execute_query(db_qa_conf, cost_services_metropolitan_interconnection_query)
            for column in cost_services_metropolitan_interconnection.columns:
                if str(cost_services_metropolitan_interconnection.dtypes[column]) == 'object':
                    cost_services_metropolitan_interconnection[column] = cost_services_metropolitan_interconnection[column].str.decode("utf-8")

            cost_services_metropolitan_interconnection_dict = dict(zip(list(cost_services_metropolitan_interconnection['range']), cost_services_metropolitan_interconnection['price']))

            Carrier_data_metropolitan_ET = Carrier_data_metropolitan[(Carrier_data_metropolitan['Tecn'] == 'ET') & (Carrier_data_metropolitan['CategoriaServicio'] == 'Interconexión Metropolitana')].reset_index(drop=True)

            for i in range(Carrier_data_metropolitan_ET['Consecutivo'].count()):
                row = [today, Carrier_data_metropolitan_ET.loc[i, 'Consecutivo'], carrier, 'Interconexion metropolitana', 'ET', int(Carrier_data_metropolitan_ET.loc[i, 'Capacidad']) * ETB.ET_cost(Carrier_data_metropolitan_ET.loc[i, 'Capacidad'], cost_services_metropolitan_interconnection_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1

            billing.to_csv('temp_table.csv', index=False, encoding='iso-8859-1')
            result = dbFuntions.append_csv_data(db_qa_conf, 'temp_table.csv', table)
            print (result)

        
        if carrier == 'IT':
            
            billing = pd.DataFrame([['','','','','','']], columns=['date_time', 'ID_service', 'carrier', 'service_category', 'type_service', 'cost'])
            sql_query = "SELECT * FROM servicesLogs WHERE Carrier = '{0}' AND Estado = 'Activo' AND (dateModificacionContrato <= '{1}')".format(carrier, today)
            Carrier_data = dbFuntions.execute_query(db_qa_conf, sql_query)
            Carrier_data = Carrier_data.sort_values(by='dateModificacionContrato', ascending=False).groupby('Consecutivo', as_index=False).first()
            print(Carrier_data['Consecutivo'].count())


            # Cálculo Agregación Nacional
            cost_services_national_aggregation_query = "SELECT Capacity AS `range`, Price AS price FROM billing_information_INTERNEXA WHERE type = 'National Aggregation'"
            cost_services_national_aggregation = dbFuntions.execute_query(db_qa_conf, cost_services_national_aggregation_query)
            for column in cost_services_national_aggregation.columns:
                if str(cost_services_national_aggregation.dtypes[column]) == 'object':
                    cost_services_national_aggregation[column] = cost_services_national_aggregation[column].str.decode("utf-8")

            cost_services_national_aggregation_dict = dict(zip(list(cost_services_national_aggregation['range']), cost_services_national_aggregation['price']))

            Carrier_data_metropolitan_ET = Carrier_data_metropolitan[(Carrier_data_metropolitan['Tecn'] == 'ET') & ((Carrier_data_metropolitan['CategoriaServicio'] == 'Interconexión Metropolitana') | 
                                                                        (Carrier_data_metropolitan['CategoriaServicio'] == 'Interconexión Metropolitana') | 
                                                                        (Carrier_data_metropolitan['CategoriaServicio'] == 'Interconexión Metropolitana'))].reset_index(drop=True)

            for i in range(Carrier_data_metropolitan_ET['Consecutivo'].count()):
                row = [today, Carrier_data_metropolitan_ET.loc[i, 'Consecutivo'], carrier, 'Interconexion metropolitana', 'ET', int(Carrier_data_metropolitan_ET.loc[i, 'Capacidad']) * IT.ET_cost(Carrier_data_metropolitan_ET.loc[i, 'Capacidad'], cost_services_national_aggregation_dict)/30]
                billing.loc[numberRow] = row
                numberRow += 1

            billing.to_csv('temp_table.csv', index=False, encoding='iso-8859-1')
            result = dbFuntions.append_csv_data(db_qa_conf, 'temp_table.csv', table)
            print (result)


    print(billing)