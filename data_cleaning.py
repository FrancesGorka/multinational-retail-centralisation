import re
import pandas as pd
from datetime import datetime
from data_extraction import DataExtractor

class DataCleaning():
    # removes NULL values, and formatting errors
    def clean_invalid_date(df,column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset = column_name,how='any',inplace= True)
        return df
    
    def clean_user_data(db_connector, user_data):
        data_extractor = DataExtractor()
        df = DataExtractor.read_rds_table(db_connector, user_data)
        df = clean_invalid_date(df,'date_of_birth')
        df = clean_invalid_date(df,'join_date')
        df['address'] = df['address'].str.replace('\n', ', ')
        df = df[df['email_address'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$')]
        df['phone_number'] = df['phone_number'].str.replace(r'\D', '', regex=True)
        return df
    
    def clean_card_data(card_data):
        card_data['card_number'] = card_data['card_number'].apply(str)
        card_data = clean_invalid_date(card_data,'date_payment_confirmed')  
        card_data = clean_invalid_date(card_data,'expiry_date')  
        return card_data
    
    def clean_continent(store_df):
        i=0
        for continent in store_df['continent']:
            if "Europe" in continent:
                store_df.at[i, 'continent'] = "Europe"
            elif "America" in continent:
                store_df.at[i, 'continent'] = "America"
            else:
                store_df = store_df.drop(i)
            i = i+1
        return store_df

    def clean_store_data(store_df):
        store_df.drop(columns='lat',inplace=True)
        store_df = clean_invalid_date(store_df,"opening_date")
        store_df['address'] = store_df['address'].str.replace('\n', ', ')
        store_df = clean_continent(store_df)
        return store_df
        

    def convert_multiple_weights(products):
        pattern = r'(\d+(\.\d+)?)\s*([kKgGmMoOzZlL]*)'
        products['weight'] = products['weight'].astype(str)
        for i, product in products.iterrows():
            if "x" in product['weight']:
                matches = re.findall(pattern, product['weight'])
                for match in matches:
                    quantity = float(match[0])
                    unit_weight = float(match[1]) if match[1] else 1.0
                    unit = match[2].lower()
                    new_weight = quantity * unit_weight
                    product['weight'] = f"{new_weight:.2f}{unit}"
                    products.at[i, 'weight'] = product['weight']
        return products

    def convert_product_weights(products):
        products = convert_multiple_weights(products)
        for index, row in products.iterrows():
            weight = str(row['weight'])
            if "kg" in weight:
                numeric_part = weight.replace("kg", "").strip()
                numeric_value = float(numeric_part)
                products.at[index, 'weight'] = numeric_value
            elif "oz" in weight:
                numeric_part = weight.replace("oz", "").strip()
                numeric_value = float(numeric_part)
                products.at[index, 'weight'] = numeric_value * 0.0283495
            elif "ml" in weight:
                # Taking the average densities of substances in ml to be 1.28 g/ml
                numeric_part = weight.replace("ml", "").strip()
                numeric_value = float(numeric_part)
                products.at[index, 'weight'] = numeric_value * 1.28
            elif "g" in weight:
                numeric_part = weight.replace("g", "").strip()
                numeric_value = float(numeric_part)
                products.at[index, 'weight'] = numeric_value / 1000
        return products
         
    def clean_products_data(products):
        # this method will clean the DataFrame of any additional erroneous values.
        df.dropna(inplace=True)
        products = convert_product_weights(products)
        products = clean_invalid_date(products,'date_added')
        return products

    def clean_orders_data(orders_table):
        columns_to_drop = ['first_name', 'last_name', '1']
        orders_table = orders_table.drop(columns=columns_to_drop)
        return orders_table

    def clean_sales_data(sales_table):
        sales_table['timestamp'] = pd.to_datetime(sales_table['timestamp'], format='%H:%M:%S')
        return sales_table

