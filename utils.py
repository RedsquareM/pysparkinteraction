 # Util function: extract features, Attributes, Labels
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

import psycopg2
import pandas as pd
from sklearn import metrics
   
    
    def extract(self, dataframe):

        # features
        features = list()
        for col in dataframe.columns:
            if str(dataframe[col].dtypes).startswith('int') or str(dataframe[col].dtypes).startswith('float'):
                if col == 'index' or col == 'code':
                    pass
                else:
                    features.append(col)
        df_features = dataframe[features]

        # attributes
        col = dataframe['product_name']
        encoded_values, unique_values = pd.factorize(col)
        df_pandas['attributes'] = encoded_values
        df_attributes = pd.DataFrame()
        df_attributes = df_pandas['attributes']


        # labels
        labels = len(list(dataframe['product_name'].unique()))

        return df_features, df_attributes, labels