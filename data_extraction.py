import pandas as pd
from database_utils import *
import tabula

class DataExtractor:

    # takes in a DatabaseConnector() object and a table name, returns the table as a pandas dataframe
    def read_rds_table(self, db_connector, table):
        
        engine = db_connector.init_db_engine()
        #table_df = pd.read_sql_table(table, engine)

        # selects everything from the table name that is passed in to the method. 
        query = (f"SELECT * FROM {table}")
        table_df = pd.read_sql_query(sql=text(query), con=engine.connect())
        
        return table_df
    
    def retrieve_pdf_data(self, link):

        table_df = tabula.read_pdf(link, pages='all')

        return table_df
