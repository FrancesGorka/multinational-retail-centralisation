import pandas as pd
import re
from data_extraction import DataExtractor

class DataCleaning:

    @staticmethod
    def clean_invalid_date(df, column_name):
        df[column_name] = pd.to_datetime(df[column_name], format='%Y-%m-%d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%Y %B %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], format='%B %Y %d', errors='ignore')
        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
        df.dropna(subset=[column_name], how='any', inplace=True)
        return df

    @staticmethod
    def clean_user_data(user_data):
        user_data = DataCleaning.clean_invalid_date(user_data, 'date_of_birth')
        user_data = DataCleaning.clean_invalid_date(user_data, 'join_date')
        user_data['address'] = user_data['address'].str.replace('\n', ', ')
        user_data = user_data[user_data['email_address'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$')]
        user_data.loc[:, 'phone_number'] = user_data['phone_number'].str.replace(r'\D', '', regex=True)
        return user_data

    @staticmethod
    def clean_card_data(card_data):
        card_data = DataExtractor.retrieve_pdf_data(card_data)
        card_data['card_number'] = card_data['card_number'].apply(str)
        card_data = DataCleaning.clean_invalid_date(card_data, 'date_payment_confirmed')
        card_data = DataCleaning.clean_invalid_date(card_data, 'expiry_date')
        return card_data

    @staticmethod
    def clean_continent(store_df):
        store_df['continent'] = store_df['continent'].apply(lambda continent: "Europe" if "Europe" in continent else ("America" if "America" in continent else None))
        store_df.dropna(subset=['continent'], inplace=True)
        return store_df

    @staticmethod
    def clean_store_data(store_df):
        store_df.drop(columns='lat', inplace=True)
        store_df = DataCleaning.clean_invalid_date(store_df, "opening_date")
        store_df['address'] = store_df['address'].str.replace('\n', ', ')
        store_df = DataCleaning.clean_continent(store_df)
        return store_df

    @staticmethod
    def convert_multiple_weights(products):
        pattern = r'(\d+(\.\d+)?)\s*([kKgGmMoOzZlL]*)'
        products['weight'] = products['weight'].astype(str)
        for i, product in products.iterrows():
            if "x" in product['weight']:
                matches = re.findall(pattern, product['weight'])
                new_weights = []
                for match in matches:
                    quantity = float(match[0])
                    unit_weight = float(match[1]) if match[1] else 1.0
                    unit = match[2].lower()
                    new_weight = quantity * unit_weight
                    new_weights.append(f"{new_weight:.2f}{unit}")
                products.at[i, 'weight'] = ' x '.join(new_weights)
        return products

    @staticmethod
    def convert_product_weights(products):
        products = DataCleaning.convert_multiple_weights(products)
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
                numeric_part = weight.replace("ml", "").strip()
                numeric_value = float(numeric_part)
                products.at[index, 'weight'] = numeric_value * 1.28
            elif "g" in weight:
                numeric_part = weight.replace("g", "").strip()
                numeric_value = float(numeric_part)
                products.at[index, 'weight'] = numeric_value / 1000
        return products

    @staticmethod
    def clean_products_data(products):
        products.dropna(inplace=True)
        products = DataCleaning.convert_product_weights(products)
        products = DataCleaning.clean_invalid_date(products, 'date_added')
        return products

    @staticmethod
    def clean_orders_data(orders_table):
        columns_to_drop = ['first_name', 'last_name', '1']
        orders_table.drop(columns=columns_to_drop, inplace=True)
        return orders_table

    @staticmethod
    def clean_sales_data(sales_table):
        sales_table['timestamp'] = pd.to_datetime(sales_table['timestamp'], format='%H:%M:%S')
        return sales_table