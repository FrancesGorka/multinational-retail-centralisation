import pandas as pd
import tabula as tb
from database_utils import DatabaseConnector

class DataExtractor():
    # This class will work as a utility class, containing methods that help extract data from CSV files, an API and an S3 bucket.

    def read_rds_table(db_connector,table_name):
        # Take an instance of DatabaseConnector class and table name, returns pandas dataframe 
        tables = db_connector.list_db_tables()
        if table_name in tables:
            engine = db_connector.init_db_engine()
            dataframe = pd.read_sql(table_name, engine)
        return dataframe

    def retrieve_pdf_data(url):
        all_pages_df = tb.read_pdf(url, pages='all')
        return all_pages_df

