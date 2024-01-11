import pandas as pd
import tabula
import requests
import boto3
from io import BytesIO
import os

class DataExtractor:

    def __init__(self):
        self.api_key = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        self.num_of_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

    def read_rds_table(self, engine, table_name):
        with engine.begin() as conn:
            return pd.read_sql_table(table_name, conn)

    def retrieve_pdf_data(self):
        os.environ['JAVA_HOME'] = '/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home'
        path = 'card_details.pdf'
        df_table = tabula.read_pdf(path, pages='all', pandas_options={'header': None})
        return df_table

    def list_number_of_stores(self):
        try:
            
            response = requests.get(self.num_of_stores_url, headers=self.api_key)
            if response.status_code == 200:
                number_of_stores = response.json()['number_stores']
                return number_of_stores
            else:
                print(f"Error: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    def retrieve_stores_data(self):
            number_of_stores = self.list_number_of_stores()
            stores_list = []
            try:
                for i in range(1, number_of_stores + 1):
                    self.store = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{i}'
                    response = requests.get(self.store, headers=self.api_key)                    
                    if response.status_code == 200:
                        stores_data = response.json()
                        stores_list.append(stores_data)
                    else:
                        print(f"Error: {response.status_code}")
                return pd.json_normalize(stores_list)

            except Exception as e:
                print(f"Error: {str(e)}")
                return None

    def extract_from_s3(self, s3_address):

        try:
            # Splitting the S3 address to get bucket and key
            s3_parts = s3_address.replace('s3://', '').split('/')
            bucket_name = s3_parts[0]
            key = '/'.join(s3_parts[1:])
            print(bucket_name, key)
            # Initialising a boto3 S3 client
            s3_client = boto3.client('s3')

            # Downloading the file from S3
            response = s3_client.get_object(Bucket='data-handling-public', Key='products.csv')
            print(response)
            file_content = response['Body'].read()
            dataFrame = pd.read_csv(BytesIO(file_content))

            return dataFrame
        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    def extract_json(self, json_url):
        dataFrame = pd.read_json(json_url)
        return dataFrame
