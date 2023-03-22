import pandas as pd
from database_utils import *
class DataExtractor:

    def read_rds_table(self, db_connector, table):
        
        engine = db_connector.init_db_engine()
        query = (f"SELECT * FROM {table}")
        table_df = pd.read_sql_query(sql=text(query), con=engine.connect())
        
        return table_df
    
