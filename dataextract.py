from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import psycopg2

import os


class DataExtract:

        # Connect to the PostgreSQL database
        try:
            self.conn = psycopg2.connect(host="localhost", port=5432, database="fooddb", user="", password="")
            # Create a connection engine to the database
            self.engine = create_engine('postgresql://postgres:postgres@localhost:5432/fooddb')

        except:
            print('Error Connecting to PostreSQL')




    def readData(self, path):

        try:
            # reading the first 1000 rows of the data
            self.df = pd.read_csv(path, sep='\t', low_memory=False, nrows=1000)
            self.df = self.df.fillna(0)


            # Write the dataframe to a table in the database
            for col in self.df.columns:
                if self.df[col].dtypes == 'np.uint64':
                    self.df[col] = self.df[col].astype('int64')
        except:
            print('Error Reading Data')





    def buildDB(self):

        try:
            self.df.to_sql('food_data', self.engine, if_exists='replace')
            print('PostreSQL is good')

        except:
            print('Error: PostgreSQL is not available')

        self.conn.close()
