import yaml
from sqlalchemy import create_engine, text

class DatabaseConnector():
    # This class can be used to connect with and upload data to the database.

    @staticmethod
    def read_db_creds():
        with open('db_creds.yaml', 'r') as f:
            yaml_data = yaml.safe_load(f)
        return yaml_data

    @staticmethod
    def init_db_engine():
        credentials = read_db_creds()

        RDS_HOST = credentials['RDS_HOST']
        RDS_PASSWORD = credentials['RDS_PASSWORD']
        RDS_USER = credentials['RDS_USER']
        RDS_DATABASE = credentials['RDS_DATABASE']
        RDS_PORT = credentials['RDS_PORT']

        connection_string = f"postgresql+psycopg2://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
        engine = create_engine(connection_string)

        return engine
    
    @staticmethod
    def list_db_tables():
        tables = []
        engine = DatabaseConnector.init_db_engine()
        with engine.connect() as connection:
            query = text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';")
            result = connection.execute(query)

        for row in result:
            tables.append(row[0])

        return tables

    def upload_to_db(dataframe,table_name):
        engine = DatabaseConnector.init_db_engine()
        dataframe.to_sql('table_name', engine, if_exists='replace', index=False)
        print(f"{table_name} has successfully been uploaded.")