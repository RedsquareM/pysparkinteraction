import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

import psycopg2
import pandas as pd
from sklearn import metrics


class Model:

    # Initialise
    def __init__(self):

        # Create a SparkSession
        self.spark = SparkSession.builder.appName("Model").getOrCreate()
        self.conn = psycopg2.connect(host="localhost", port=5432, database="fooddb", user="", password="")

        try :
            self.df_pandas = pd.read_sql_query("SELECT * FROM food_data", self.conn)
            self.df_pandas = self.df_pandas.fillna(0)

        except:
            print('Error: No Data')

        self.conn.close()



    # perform Clustering
    def main(self):

        features, attributes, labels = self.extract(self.df_pandas)

        # features columns 
        features_cols = features.columns
        output_col = "features"

        # Create a VectorAssembler transformer
        assembler = VectorAssembler(inputCols=features_cols, outputCol=output_col)

        # Transform the data
        features_df = spark.createDataFrame(features)
        features_df = assembler.transform(features_df)

        # Create a KMeans model
        kmeans = KMeans(featuresCol=output_col, k=labels)

        # Fit the model to the data
        model = kmeans.fit(features_df)

        # Get the predictions
        predictions = model.transform(features_df)

        # Evalution
        y_true = attributes

        preds = predictions.toPandas()
        y_pred = preds['prediction'].values

        acc_score_ = metrics.accuracy_score(y_true, y_pred)
        f1_score_ = metrics.f1_score(y_true, y_pred, average='macro')
        c_matrix_ = metrics.confusion_matrix(y_true, y_pred)

        predictions.show()

        print(f'Accuracy Score: {acc_score_}')
        print(f'F1 Score: {f1_score_}')
        print(f'Confusion Matrix: \n{c_matrix_}')


   