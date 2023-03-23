import yaml
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy import inspect
import numpy as np


class DatabaseConnector:
    
    # opens yaml file containing db credentials and saves them in a python dict, returns the dict
    def read_db_creds(self):

        with open('db_creds.yaml', 'r')  as db_creds:
            db_dict = yaml.safe_load(db_creds)
        
        return db_dict
    
    # initialises a sqlalchemy engine using the db creds and returns the engine
    def init_db_engine(self):

        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        engine.connect()
        return engine

    # lists the tables in the db
    def list_db_tables(self):

        engine = self.init_db_engine()

        inspector = inspect(engine)
        table_names = inspector.get_table_names()

        print(table_names)

    # takes in a pandas dataframe and a table name and uploads that dataframe as a table to the database
    def upload_to_db(self, df, table_name):
        
        engine = create_engine("postgresql+psycopg2://postgres:70Bahawalpur@localhost:5432/Sales_Data")
        engine.connect()
        df.to_sql(table_name, engine, if_exists='replace', index=False, index_label='index')









        

        
