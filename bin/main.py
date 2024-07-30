import tensorflow_datasets as tfds
import tensorflow as tf
from search_engine_scoredidx import ScoredIndexSearch

# Check if the dataset is installed
if "movielens/25m-movies" not in tfds.list_builders():
    print("Dataset not installed. Installing now...")
    tfds.load("movielens/25m-movies", download=True)

# Download the MovieLens dataset
movies, info = tfds.load("movielens/25m-movies", split="train", with_info=True)

# Check if the dataset is installed
if "movielens/25m-ratings" not in tfds.list_builders():
    print("Dataset not installed. Installing now...")
    tfds.load("movielens/25m-ratings", download=True)

# Download the MovieLens dataset
ratings, info = tfds.load("movielens/25m-ratings", split="train", with_info=True)

# Create a search engine
t = ScoredIndexSearch("search", "localhost")
t.connection.ping()  # Check if the Redis server is available

# Get the list of keys and delete them properly
keys = t.connection.keys("search*")
if keys:
    t.connection.delete(*keys)

# Index the dataset
for movie in movies:
    movie_id = movie["movie_id"].numpy()
    movie_title = movie["movie_title"].numpy().decode('utf-8')
    t.add_indexed_item(movie_id, movie_title)

# Search for a movie
print(t.search("Toy Story"))
print(t.search("The Matrix"))
print(t.search("Star Wars"))
print(t.search("The Lord of the Rings"))

# Close the connection
t.connection.close()
