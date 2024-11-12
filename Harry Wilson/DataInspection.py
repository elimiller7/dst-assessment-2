from pyspark.sql import SparkSession

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

# Load each dataset using the generalized file path
# Example loading user_artists.dat
user_artists_df = load_data("user_artists.dat")
artists_df = load_data("artists.dat")
tags_df = load_data("tags.dat")
user_taggedartists_df = load_data("user_taggedartists.dat")
user_friends_df = load_data("user_friends.dat")
user_taggedartists_timestamps_df = load_data("user_taggedartists-timestamps.dat")

# Example: Displaying the first few rows of another dataset
user_artists_df.show(3)
artists_df.show(3)
tags_df.show(3)
user_taggedartists_df.show(3)
user_friends_df.show(3)
user_taggedartists_timestamps_df(3)

