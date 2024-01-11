import yaml
from sqlalchemy import create_engine, inspect
import psycopg2
from data_extraction import DataExtractor
import pandas as pd

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            return yaml.safe_load(file)
    
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        database_url = f"postgresql://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        engine = create_engine(database_url)
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        db_tables = inspector.get_table_names()
        return db_tables

    def upload_to_db(self, dataFrame, table_name):
        local_url = f"postgresql://postgres:EROsion199@localhost:5432/sales_data"
        engine = create_engine(local_url)
        if type(dataFrame) == list:
            df = pd.concat(dataFrame, ignore_index=True)
            df.to_csv('output.csv', index=False)
            return df

        
        else:
            return dataFrame.to_sql(table_name, engine, if_exists='replace')
