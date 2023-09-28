import pandas as pd
import tabula
import requests
import boto3
import io

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_rds_table(self, table_name, creds):
        tables = self.db_connector.list_db_tables()
        if table_name in tables:
            engine = self.db_connector.init_db_engine(creds)
            dataframe = pd.read_sql(table_name, engine)
            return dataframe
        else:
            print("Table not found.")
            return None

    @staticmethod
    def retrieve_pdf_data(url):
        all_pages_df = tb.read_pdf(url, pages='all')
        combined_df = pd.concat(all_pages_df, ignore_index=True)
        return combined_df

    @staticmethod
    def list_number_of_stores(endpoint, header_dict):
        response = requests.get(endpoint, headers=header_dict)
        if response.status_code == 200:
            return response.json()['number_stores']
        else:
            print("Failed to fetch the number of stores")
            return 0
    
    @staticmethod
    def retrieve_stores_data(endpoint, header_dict, no_of_stores):
        store_data_list = []

        for store in range(no_of_stores):
            full_endpoint = endpoint.format(store)
            response = requests.get(full_endpoint, headers=header_dict)
            if response.status_code == 200:
                store_data_list.append(response.json())
            else:
                print(f"Failed to retrieve data for store {store}")
                store_data_list.append(0)

        stores_df = pd.DataFrame(store_data_list)
        return stores_df

    @staticmethod
    def extract_from_s3(s3_address):
        s3 = boto3.client('s3')
        bucket, key = s3_address.replace('s3://', '').split('/', 1)
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response['Body'].read()
            df = pd.read_csv(io.BytesIO(body))
            return df
        except Exception as e:
            print(f"Failed to extract data from S3: {str(e)}")
            return None
