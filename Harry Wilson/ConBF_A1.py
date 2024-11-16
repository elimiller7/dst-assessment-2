from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.stat import Correlation

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Content-Based Filtering") \
    .getOrCreate()

# Define the base directory for your dataset files
base_directory = "/Users/harrywilson/Desktop/DataScienceToolbox/Assessment2Data"

# Function to load a .dat file as a DataFrame
def load_data(filename):
    file_path = f"{base_directory}/{filename}"
    return spark.read.csv(file_path, sep="\t", header=True, inferSchema=True)

# Step 1: Load datasets
user_artists_df = load_data("user_artists.dat")
artists_df = load_data("artists.dat")
tags_df = load_data("tags.dat")
user_taggedartists_df = load_data("user_taggedartists.dat")

# Join user_taggedartists with tags for tag information
artist_tags_df = user_taggedartists_df.join(tags_df, on="tagID", how="inner")

# Join artist tags with artists to get artist details and tag names
artist_tags_info_df = artist_tags_df.join(
    artists_df, artist_tags_df.artistID == artists_df.id
).select(
    artist_tags_df["artistID"],
    artists_df["name"].alias("artist_name"),
    artist_tags_df["tagValue"].alias("tag")
)

# Step 2: Aggregate tags for each artist into a list and remove duplicates
artist_profiles_df = artist_tags_info_df.groupBy("artistID", "artist_name") \
    .agg(F.collect_set("tag").alias("tags"))

# Display artist profiles with unique tags list
artist_profiles_df.show(5, truncate=False)

# Ensuring tax_text is an array of strings
tags_df = artist_profiles_df.select(
    F.col("artistID"),
    F.col("artist_name"),
    F.col("tags").alias("tag_text")  # Keep tags as an array of strings
)


vectorizer = CountVectorizer(inputCol="tag_text", outputCol="raw_features")
vectorized_model = vectorizer.fit(tags_df)
vectorized_df = vectorized_model.transform(tags_df)

# Step 4: Compute TF-IDF to weigh the importance of tags
idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(vectorized_df)
tfidf_df = idf_model.transform(vectorized_df)

# Step 5: Normalize the feature vectors
scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(tfidf_df)
scaled_tfidf_df = scaler_model.transform(tfidf_df)

# Step 6: Compute similarities between artists using cosine similarity
artist_features_rdd = scaled_tfidf_df.rdd.map(lambda row: (row.artistID, row.scaled_features))
artist_features = artist_features_rdd.collectAsMap()

def cosine_similarity(vec1, vec2):
    dot_product = vec1.dot(vec2)
    norm1 = Vectors.norm(vec1, 2)
    norm2 = Vectors.norm(vec2, 2)
    return dot_product / (norm1 * norm2) if norm1 != 0 and norm2 != 0 else 0

# Build similarity matrix
similarity_rdd = spark.sparkContext.parallelize([
    (id1, id2, cosine_similarity(features1, features2))
    for id1, features1 in artist_features.items()
    for id2, features2 in artist_features.items()
    if id1 != id2
])

similarity_df = similarity_rdd.toDF(["artistID_1", "artistID_2", "similarity"])

# Step 7: Recommend similar artists for each user based on the content similarity
user_artist_df = user_artists_df.join(artists_df, user_artists_df.artistID == artists_df.id).select(
    user_artists_df["userID"], 
    user_artists_df["artistID"]
)

user_recommendations = user_artist_df.join(similarity_df, user_artist_df.artistID == similarity_df.artistID_1) \
    .groupBy("userID", "artistID_2") \
    .agg(F.mean("similarity").alias("avg_similarity")) \
    .orderBy("userID", "avg_similarity", ascending=False)

# Step 8: Display final recommendations
user_recommendations.show(10, truncate=False)

# Optional: Save results for further analysis
user_recommendations.write.csv(f"{base_directory}/content_based_recommendations.csv", header=True)

print("Content-based filtering completed.")
