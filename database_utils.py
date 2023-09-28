import yaml
from sqlalchemy import create_engine, text

class DatabaseConnector:
    
    @staticmethod
    def read_db_creds(creds):
        with open(creds, 'r') as f:
            yaml_data = yaml.safe_load(f)
        return yaml_data

    @staticmethod
    def init_db_engine(creds):
        credentials = DatabaseConnector.read_db_creds(creds)

        HOST = credentials['HOST']
        PASSWORD = credentials['PASSWORD']
        USER = credentials['USER']
        DATABASE = credentials['DATABASE']
        PORT = credentials['PORT']

        connection_string = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)

        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine("db_creds.yaml")
        tables = []
        with engine.connect() as connection:
            query = text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';")
            result = connection.execute(query)

        for row in result:
            tables.append(row[0])

        return tables

    def upload_to_db(self, df, table_name):
        try:
            creds = "sales_db_creds.yaml"
            engine = self.init_db_engine(creds)
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"Table '{table_name}' has been created in the database.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {str(e)}")