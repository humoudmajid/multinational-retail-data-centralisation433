from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd


data_cleaing = DataCleaning()
data_extraction = DataExtractor()
database_conn = DatabaseConnector()

def upload_to_dim_users():

    #Initialises and returns an sqlalchemy database engine.
    engine = database_conn.init_db_engine()
    #Reads the data from the RDS database to a pandas dataFrame.
    df_tables = data_extraction.read_rds_table(engine, 'legacy_users')
    #Cleans data
    cleaned_user_data = data_cleaing.clean_user_data(df_tables)
    #Data upload 
    return database_conn.upload_to_db(cleaned_user_data, 'dim_users')
#upload_to_dim_users()


def upload_to_dim_card_details():
    #Reads the data from PDF and converts to a pandas dataFrame.
    df_tables = data_extraction.retrieve_pdf_data()
    #Cleans data
    cleaned_card_data = data_cleaing.clean_card_data(df_tables)
    #Data upload 
    return database_conn.upload_to_db(cleaned_card_data, 'dim_card_details')
#upload_to_dim_card_details()

def upload_to_dim_store_details():
    #Returns number of stores
    num_of_stores = data_extraction.list_number_of_stores()
    #Retrieves all store data
    df_stores = data_extraction.retrieve_stores_data()
    #Cleans Data
    cleaned_store_data = data_cleaing.clean_store_data(df_stores)
    print(cleaned_store_data)
    #Data upload 
    return database_conn.upload_to_db(cleaned_store_data, 'dim_store_details')
#upload_to_dim_store_details()

def upload_to_dim_products():
    s3_address = 's3://data-handling-public/products.csv'
    #Reads the data from PDF and converts to a pandas dataFrame.
    df_tables = data_extraction.extract_from_s3(s3_address)
    #Cleans data
    #converted_products_data = data_cleaing.convert_product_weights(df_tables)
    #cleaned_products_data = converted_products_data.clean_products_data(df_tables)
    print(df_tables)
    #Data upload 
    #return database_conn.upload_to_db(cleaned_products_data, 'dim_products')
upload_to_dim_products()

def upload_to_orders_table():

    #Initialises and returns an sqlalchemy database engine.
    engine = database_conn.init_db_engine()
    #Reads the data from the RDS database to a pandas dataFrame.
    df_tables = data_extraction.read_rds_table(engine, 'orders_table')
    #Cleans data
    cleaned_orders_data = data_cleaing.clean_orders_data(df_tables)

    #Data upload 
    return database_conn.upload_to_db(cleaned_orders_data, 'orders_table')
#upload_to_orders_table()

def upload_to_dim_date_times():
    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    dataFrame = data_extraction.extract_json(json_url)
    return database_conn.upload_to_db(dataFrame, 'dim_date_times')
#upload_to_dim_date_times()
