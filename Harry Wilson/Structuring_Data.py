from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LastFM Analysis") \
    .getOrCreate()

# Define the base directory for your dataset files
base_directory = "/Users/harrywilson/Desktop/DataScienceToolbox/Assessment2Data"

# Function to load a .dat file as a DataFrame
def load_data(filename):
    file_path = f"{base_directory}/{filename}"
    return spark.read.csv(file_path, sep="\t", header=True, inferSchema=True)


# Step 1: Load datasets as before
user_artists_df = load_data("user_artists.dat")
artists_df = load_data("artists.dat")
tags_df = load_data("tags.dat")
user_taggedartists_df = load_data("user_taggedartists.dat")

# Join user_taggedartists with tags for tag information
artist_tags_df = user_taggedartists_df.join(tags_df, on="tagID", how="inner")

# Join artist tags with artists to get artist details and tag names
artist_tags_info_df = artist_tags_df.join(artists_df, artist_tags_df.artistID == artists_df.id).select(
    artist_tags_df["artistID"],
    artists_df["name"].alias("artist_name"),
    artist_tags_df["tagValue"].alias("tag")
)

# Display combined data with tags for each artist
artist_tags_info_df.show(5)

# Aggregate tags for each artist into a list
artist_profiles_df = artist_tags_info_df.groupBy("artistID", "artist_name") \
    .agg(F.collect_set("tag").alias("tags"))

# Display artist profiles with tags list
artist_profiles_df.show(5, truncate=False)

# Join user listening data with artist profiles
user_artist_profile_df = user_artists_df.join(artist_profiles_df, on="artistID", how="inner")

# Display the combined dataset
user_artist_profile_df.show(5, truncate=False)

# Split the user_artist_profile_df into train and test sets (80% train, 20% test)
train_data, test_data = user_artist_profile_df.randomSplit([0.80, 0.20], seed=42)

# Check the number of rows in each dataset to verify the split
print("Train Data Count: ", train_data.count())
print("Test Data Count: ", test_data.count())

train_data.show(20, truncate=False)
test_data.show(20, truncate=False)
