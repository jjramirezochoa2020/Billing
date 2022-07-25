import unicodedata
import numpy as np
import pandas as pd
import mysql.connector
import scrapper_TRM as TRM
from datetime import datetime
from datetime import timedelta
import billing_functions as bf
import sql_db_functions as dbFuntions


number_row = 0
table = 'billing_carriers'
carrier_list = ['CW', 'UFINET', 'ETB', 'INTERNEXA', 'GTD', 'AXESS']
db_qa_conf = {'user':'collector_qa','password':'WOM1234','host':'10.40.111.103','database':'BILLING'}


for day in range(100,170):

    # Crear un dataframe vacío "billing"
    billing = pd.DataFrame([['','','','','','','',0]], columns=['date_time', 'ID_service', 'carrier', 'service_category', 'type_service', 'capacity', 'rate', 'cost'])
    billing['cost'] = billing['cost'].astype('float')

    print(f'Loop day {day}')
    current_date = datetime.today()
    year = current_date.year
    month = current_date.month
    today = (datetime.now() - timedelta(days=day)).strftime('%Y-%m-%d')

    for carrier in carrier_list:

        print(f'Loop carrier {carrier}')
        if carrier == 'CW':

            # Servicios del carrier
            Carrier_data = bf.get_carrier_services(carrier, today, db_qa_conf)

            # Calcular los servicios E1 y STM1 de interconexión nacional
            service_category = ['Interconexión Nacional']
            database_filter = "national E1"
            billing, number_row = bf.get_E1_STM1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los servicios E1 y STM1 de interconexión metropolitana
            service_category = ['Interconexión Metropolitana']
            database_filter = "metropolitan E1"
            billing, number_row = bf.get_E1_STM1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de interconexión metropolitana
            service_category = ['Interconexión Metropolitana']
            database_filter = "Point to Point metropolitan aggregation"
            billing, number_row = bf.get_ET_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)
    
            # Calcular los enlaces ET de agregación metropolitana
            service_category = ['Enlaces Agregación Metropolitana']
            database_filter = "Metropolitan Aggregation"
            billing, number_row = bf.get_aggregation_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces especiales 
            service_category = ['IP Transit']
            database_filter = "IP Transit service"
            billing, number_row = bf.get_special_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)
            

        if carrier == 'UFINET':

            # Servicios del carrier
            Carrier_data = bf.get_carrier_services(carrier, today, db_qa_conf)

            # Calcular los servicios de Agregaciones metropolitanas
            service_category = ['Enlaces Agregación Metropolitana']
            database_filter = "Metropolitan Aggregation"
            billing, number_row = bf.get_aggregation_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los servicios de Agregaciones nacionales
            service_category = ['Enlaces Agregación Nacional', 'Enlaces Especiales']
            database_filter = "National Aggregation"
            billing, number_row = bf.get_aggregation_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

        
        if carrier == 'ETB':
    
            # Servicios del carrier
            Carrier_data = bf.get_carrier_services(carrier, today, db_qa_conf)

            # Calcular los servicios E1 y STM1 de interconexión nacional
            service_category = ['Interconexión Nacional']
            database_filter = "national E1"
            billing, number_row = bf.get_E1_STM1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de interconexión nacional
            service_category = ['Interconexión Nacional']
            database_filter = "Point to Point national aggregation"
            billing, number_row = bf.get_ET_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los servicios E1 y STM1 de interconexión metropolitana
            service_category = ['Interconexión Metropolitana']
            database_filter = "metropolitan E1"
            billing, number_row = bf.get_E1_STM1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de interconexión metropolitana
            service_category = ['Interconexión Metropolitana']
            database_filter = "Point to Point metropolitan aggregation"
            billing, number_row = bf.get_ET_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de agregación metropolitana
            service_category = ['Enlaces Agregación Metropolitana']
            database_filter = "Metropolitan Aggregation"
            billing, number_row = bf.get_aggregation_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de interconexión metropolitana
            service_category = ['E1-T']
            database_filter = "E1 Special Rate"
            billing, number_row = bf.get_special_E1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de interconexión metropolitana
            # service_category = ['Enlaces Agregación Metropolitana']
            # database_filter = "Metropolitan Aggregation"
            # billing, number_row = bf.get_aggregation_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)
    
            # Calcular los enlaces especiales 
            # service_category = ['IP Transit']
            # database_filter = "IP Transit service"
            # billing, number_row = bf.get_special_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

        
        if carrier == 'INTERNEXA':

            # Servicios del carrier
            Carrier_data = bf.get_carrier_services(carrier, today, db_qa_conf)

            # Calcular los servicios E1 y STM1 de interconexión nacional
            service_category = ['Interconexión Nacional']
            database_filter = "national E1"
            billing, number_row = bf.get_E1_STM1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de interconexión nacional
            service_category = ['Interconexión Nacional']
            database_filter = "National Aggregation"
            billing, number_row = bf.get_ET_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los servicios E1 y STM1 de interconexión metropolitana
            service_category = ['Interconexión Metropolitana']
            database_filter = "metropolitan E1"
            billing, number_row = bf.get_E1_STM1_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los enlaces ET de interconexión metropolitana
            service_category = ['Interconexión Metropolitana']
            database_filter = "Point to Point metropolitan aggregation"
            billing, number_row = bf.get_ET_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)

            # Calcular los servicios de Agregaciones nacionales
            service_category = ['Enlaces Agregación Nacional', 'Enlaces Especiales']
            database_filter = "National Aggregation"
            billing, number_row = bf.get_ET_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)
            

        if carrier == 'GTD':
    
            # Servicios del carrier
            Carrier_data = bf.get_carrier_services(carrier, today, db_qa_conf)
            
            # Calcular los servicios de Agregaciones metropolitanas
            service_category = ['Enlaces Agregación Metropolitana']
            database_filter = "Metropolitan Aggregation"
            billing, number_row = bf.get_GTD_calculation(Carrier_data, number_row, billing, service_category, carrier, database_filter, db_qa_conf, today)


        if carrier == 'AXESS':
        
            # Servicios del carrier
            Carrier_data = bf.get_carrier_services(carrier, today, db_qa_conf)
            
            # Calcular los servicios de Agregaciones satelitales
            service_category = ['Enlaces Agregación Satelital']
            database_filter = "Satellital Aggregation"
            TRM = 3800 # TRM.get_TRM()
            billing, number_row = bf.get_ET_calculation_TRM(Carrier_data, number_row, billing, TRM, service_category, carrier, database_filter, db_qa_conf, today)

    billing = billing.dropna(subset=['ID_service'])
    billing.to_csv('temp_table.csv', index=False, encoding='iso-8859-1')
    result = dbFuntions.append_csv_data(db_qa_conf, 'temp_table.csv', table)
    print(billing)