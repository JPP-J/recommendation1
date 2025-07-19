from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, collect_set, udf
from pyspark.sql.types import FloatType
from pyspark.ml.recommendation import ALS
from pyspark.ml.recommendation import ALSModel
import pandas as pd
import numpy as np
from utils.pyspark_utils import initial_spark_session, df_csv_to_pq, df_parquet, check_table, train_test, evaluate_model, precision_at_k, save_model, load_model





if __name__=="__main__":

    # # ==================== DATA PREPARATION ===============================
    path = '/home/jpp/projects/05_reccomd1_cp/spark-warehouse/reccmd_ratings_parquet'
    path2 = 'hdfs://localhost:9000/data/reccmd_ratings_parquet'
    
    database = "default"
    table_name = 'reccmd_ratings_parquet'
    full_table_name = f"{database}.{table_name}"
    table_temp = "parquet_table"

    spark = initial_spark_session(appName='book_data')
    # df_csv_to_pq(spark,  detail=False)
    df = df_parquet(spark, path, full_table_name, table_temp, source='sp', detail=False)
    check_table(spark)

    # Example query
    print("\nrunning example query.........................")

    query = """ SELECT  book_id,
                        ROUND(AVG(rating), 0) AS avg_rating
                FROM    parquet_table
                GROUP BY book_id 
                """
    result = spark.sql(query)
    result.show(10)

     # # ==================== TRAINING ===============================

    # # Train and Test Model
    # model, train_data, test_data = train_test(spark, path="hdfs://localhost:9000/data/reccmd_ratings_parquet", show_result=False)
    # evaluate_model(model, test_data)

    # # Save and Load Model
    # save_model(model)                                                                     # Save Model                                       
    # model = load_model(user_rec=True, item_rec=False)                                     # Load Model

    # spark.stop()




