from database_utils import * 
from data_extraction import *

class DataCleaning:
    
    def clean_user_data(self):
        
        db_connect = DatabaseConnector()

        db_extract = DataExtractor()
        table =  db_extract.read_rds_table(db_connect, 'legacy_users')

        # Removes duplicate values from table
        table.drop_duplicates(inplace=True)

        return table
    

db_connect = DatabaseConnector()

db_extract = DataExtractor()
table =  db_extract.read_rds_table(db_connect, 'legacy_users').set_index('index')

print(table.head(20))
print(table.isna().sum().sum())
print(table.keys())

table.to_excel("converted-to-excel.xlsx")