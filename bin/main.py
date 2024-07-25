import tensorflow_datasets as tfds
import tensorflow
from search_engine_scoredidx import ScoredIndexSearch

# Check if the dataset is installed
if "movielens/25m-movies" not in tfds.list_builders():
    print("Dataset not installed. Installing now...")
    tfds.load("movielens/25m-movies", download=True)

# Download the MovieLens dataset
movies, info = tfds.load("movielens/25m-movies", split="train", with_info=True)

# Print dataset information
print(info)

# Create a search engine
t = ScoredIndexSearch("search", "localhost")
t.connection.ping()  # Check if the Redis server is available

# Get the list of keys and delete them properly
keys = t.connection.keys("search*")
if keys:
    t.connection.delete(*keys)

# Index the dataset
for movie in movies:
    t.add_indexed_item(movie["movie_id"], movie["movie_title"])

# Search for a movie
print(t.search("Toy Story"))
print(t.search("The Matrix"))
print(t.search("Star Wars"))
print(t.search("The Lord of the Rings"))

# Remove a movie from the index
t.remove_indexed_item(1, "Toy Story")
print(t.search("Toy Story"))
print(t.search("The Matrix"))

# Close the connection
t.connection.close()
