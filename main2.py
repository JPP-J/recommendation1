from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, collect_set, udf
from pyspark.sql.types import FloatType
from pyspark.ml.recommendation import ALS
from pyspark.ml.recommendation import ALSModel
import pandas as pd
import numpy as np
from utils.pyspark_utils import initial_spark_session, df_csv_to_pq, df_parquet, check_table, train_test, evaluate_model, precision_at_k, save_model, load_model



# def train_test(spark, path, show_result=False):
#     print("\nrunning train and test.........................")
#     df = spark.read.parquet(path)
#     df = df.withColumnRenamed("book_id", "item_id")
#     # Split data into train and test sets (80% train, 20% test)
#     train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

#     # Build ALS model (Alternating Least Squares) collaborative filtering algorithm 
#     als = ALS(
#         maxIter=10, 
#         regParam=0.1, 
#         userCol="user_id", 
#         itemCol="item_id", 
#         ratingCol="rating", 
#         coldStartStrategy="drop"
#     )

#     # Train the model
#     model = als.fit(train_data)

#     # # Generate predictions
#     # predictions = model.transform(test_data)

#     # # Show sample predictions
#     # predictions.show(5)

#     # Recommend Top N Items for Each User
#     user_recommendations = model.recommendForAllUsers(10)  # Top 10 items per user

#     # Recommend Top N Users for Each Item
#     item_recommendations = model.recommendForAllItems(10)  # Top 10 users per item
    
#     # Show results
#     if show_result == True:
#         user_recommendations.show(5, False)  # Show without truncation
#         item_recommendations.show(5, False)

#     return model, train_data, test_data


# def evaluate_model(model, test_data):
#     print("\nrunning evaluate model.........................")
#     # Recomnendation Items
#     # Recommend Top N Items for Each User
#     user_recommendations = model.recommendForAllUsers(10)  # Top 10 items per user

#     # Extract only item IDs from recommendations list
#     user_recommendations = user_recommendations.withColumn(
#         "recommended_items", expr("transform(recommendations, x -> x.item_id)")
#     )
#     user_recommendations.select("user_id", "recommended_items").show(5, False)

#     # Actual Items
#     # Group test_data by user_id and collect actual items the user interacted with
#     true_items_df = test_data.groupBy("user_id").agg(collect_set("item_id").alias("true_items"))
#     true_items_df.show(5, True)

#     # Join recommended and true items on user_id
#     evaluation_df = user_recommendations.join(true_items_df, on="user_id", how="inner")
#     evaluation_df.select("user_id", "recommended_items", "true_items").show(5, True)

#     # Register as Spark UDF
#     precision_udf = udf(lambda rec, act: precision_at_k(rec, act, 10), FloatType())

#     # Apply function
#     evaluation_df = evaluation_df.withColumn("precision_at_k", precision_udf(col("recommended_items"), col("true_items")))
#     evaluation_df.show(5, True)

#     # Show results
#     evaluation_df.select("user_id", "precision_at_k").show(10, False)


#     # Define UDF for precision_at_k
# def precision_at_k(recommended, actual, k=10):
#     print("\nrunning get precision at k.........................")
#     recommended = recommended[:k]  # Take top K
#     if not actual:  # Avoid division by zero
#         return 0.0
#     hits = sum(1 for item in recommended if item in actual)
#     return hits / k


# def save_model(model):
#     print("\nrunning save model.........................")
#     model.save("hdfs://localhost:9000/model/als_recommendation")

# def load_model(user_rec=True, item_rec=True):
#     print("\nrunning laod model.........................")
#     loaded_model = ALSModel.load("hdfs://localhost:9000/model/als_recommendation")


#     if user_rec == True:
#         user_recommendations = loaded_model.recommendForAllUsers(10)
#         user_recommendations.show(5, False)
#     else:
#         pass
    
#     if item_rec == True:
#         # Recommend Top N Users for Each Item
#         item_recommendations = loaded_model.recommendForAllItems(10)  # Top 10 users per item
#         item_recommendations.show(5, False)
#     else:
#         pass

#     return loaded_model


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




