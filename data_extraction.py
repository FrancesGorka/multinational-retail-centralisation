import pandas as pd
import tabula as tb
import requests
import boto3
import io

class DataExtractor:
    def read_rds_table(self, db_connector, table_name):
        tables = db_connector.list_db_tables()
        if table_name in tables:
            engine = db_connector.init_db_engine()
            dataframe = pd.read_sql(table_name, engine)
            return dataframe
        else:
            print("Table not found.")
            return None

    def retrieve_pdf_data(self, url):
        all_pages_df = tb.read_pdf(url, pages='all')
        return all_pages_df

    def list_number_of_stores(self, endpoint, header_dict):
        response = requests.get(endpoint, headers=header_dict)
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed to fetch the number of stores")
            return 0

    def retrieve_stores_data(self, endpoint, header_dict, no_of_stores):
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

    def extract_from_s3(self, s3_address):
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
