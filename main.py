from database_utils  import DatabaseConnector 
from data_extraction import DataExtractor 
from data_cleaning   import DataCleaning

# Initialize database connector, extractor, and cleaner
db_connector = DatabaseConnector()
db_extractor = DataExtractor(db_connector)  # Pass the db_connector instance to DataExtractor
db_cleaner = DataCleaning()

# Define API endpoints and headers
no_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_retrieval_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

# Define data sources
card_data_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
s3_products = "s3://data-handling-public/products.csv"
s3_sales = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

# Extract and clean user data
user_table = db_extractor.read_rds_table('legacy_users','db_creds.yaml')
cleaned_user_data = db_cleaner.clean_user_data(user_table)
db_connector.upload_to_db(cleaned_user_data, 'dim_users')

# Extract and clean card data
cleaned_card_data = db_cleaner.clean_card_data(card_data_link)
db_connector.upload_to_db(cleaned_card_data,'dim_card_details')

# Extract and clean store data
no_of_stores = db_extractor.list_number_of_stores(no_of_stores_endpoint, header_dict)
store_data = db_extractor.retrieve_stores_data(store_retrieval_endpoint, header_dict, no_of_stores)
cleaned_store_data = db_cleaner.clean_store_data(store_data)
db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')

# Extract and clean products data
products_data = db_extractor.extract_from_s3(s3_products)
cleaned_products_data = db_cleaner.clean_products_data(products_data)
db_connector.upload_to_db(cleaned_products_data, 'dim_products')

# Extract and clean orders data
orders_table = db_extractor.read_rds_table(db_connector, 'orders_table')
cleaned_orders_table = db_cleaner.clean_orders_data(orders_table)
db_connector.upload_to_db(cleaned_orders_table, 'dim_orders')

# Extract and clean sales data
sales_data = db_extractor.extract_from_s3(s3_sales)
cleaned_sales_data = db_cleaner.clean_sales_data(sales_data)
db_connector.upload_to_db(cleaned_sales_data, 'dim_date_times')