from pyspark.sql import SparkSession
import tensorflow as tf
from tensorflow.keras import layers, models
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import array_sort, col, expr

spark = SparkSession.builder.appName("RecommenderSystem").getOrCreate()
data = spark.read.csv("last_fm_data/user_artists.dat", header=True, inferSchema=True, sep='\t')
data = data.limit(500)

user_item_df = data.groupBy("userID").pivot("artistID").agg({"weight": "max"}).fillna(0)
user_item_pd = user_item_df.toPandas()
user_item_matrix = user_item_pd.to_numpy()
min_value = data.agg(F.min("weight")).collect()[0][0]
# This tells us that the minimum weight in the whole dataset is 13

input_dim = user_item_matrix.shape[1]
encoding_dim = 50

input_layer = layers.Input(shape=(input_dim,))
encoded = layers.Dense(encoding_dim, activation='relu')(input_layer)
decoded = layers.Dense(input_dim, activation='sigmoid')(encoded)

autoencoder = models.Model(input_layer, decoded)
autoencoder.compile(optimizer='adam', loss='mse')

autoencoder.fit(user_item_matrix, user_item_matrix,
                epochs=50,
                batch_size=256,
                shuffle=True,
                validation_split=0.1)

# Create the encoder model (only the encoding part)
encoder = models.Model(input_layer, encoded)

# Generate latent representations for users
user_latent_matrix = encoder.predict(user_item_matrix)

# Optional: Generate reconstructed recommendations
reconstructed_matrix = autoencoder.predict(user_item_matrix)

reconstructed_df = pd.DataFrame(reconstructed_matrix, columns=user_item_pd.columns)
reconstructed_df['userID'] = user_item_pd['userID']  # retain user IDs

# Convert back to Spark DataFrame
reconstructed_spark_df = spark.createDataFrame(reconstructed_df)
reconstructed_spark_df.show(5)



