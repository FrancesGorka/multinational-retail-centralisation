from database_extraction import DataExtractor

class DataCleaning():
    # removes NULL values, and formatting errors
    def clean_user_data(user_data):
        data_extractor = DataExtractor()
        df = DataExtractor.read_rds_table()
        df_cleaned = df.dropna() # drops null values
        df['Birth_Date'] = pd.to_datetime(df['Birth_Date'],format = "%Y%m%d") # standardises dates
        df['Registration_Date'] = pd.to_datetime(df['Registration_Date'],format = "%Y%m%d") # standardises dates
        df['Address'] = df['Address'].str.replace('\n', ' ')    # changes newline chars to spaces in address column
        return dataframe_cleaned

