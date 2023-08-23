from database_utils import DatabaseConnector

database_connector = DatabaseConnector

database_connector.upload_to_db(dataframe_cleaned,dim_card_details)





Create a db_creds.yaml file containing the database credentials, they are as follows:


RDS_HOST: data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com
RDS_PASSWORD: AiCore2022
RDS_USER: aicore_admin
RDS_DATABASE: postgres
RDS_PORT: 5432


You should add your db_creds.yaml file to the .gitignore file in your repository, so that the database credentials are not uploaded to your public GitHub repository.
If you don't currently have a .gitignore file, you can create one by typing touch .gitignore in the terminal. Then just add the names of any files you don't want git to track.


Now you will need to develop methods in your DatabaseConnector class to extract the data from the database.


Step 2:

Create a method read_db_creds this will read the credentials yaml file and return a dictionary of the credentials.
You will need to pip install PyYAML and import yaml to do this.


Step 3:

Now create a method init_db_engine which will read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine.


Step 4:

Using the engine from init_db_engine create a method list_db_tables to list all the tables in the database so you know which tables you can extract data from.
Develop a method inside your DataExtractor class to read the data from the RDS database.


Step 5:

Develop a method called read_rds_table in your DataExtractor class which will extract the database table to a pandas DataFrame.

It will take in an instance of your DatabaseConnector class and the table name as an argument and return a pandas DataFrame.
Use your list_db_tables method to get the name of the table containing user data.
Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.


Step 6:

Create a method called clean_user_data in the DataCleaning class which will perform the cleaning of the user data.

You will need clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.


Step 7:

Now create a method in your DatabaseConnector class called upload_to_db. This method will take in a Pandas DataFrame and table name to upload to as an argument.


Step 8:

Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users.