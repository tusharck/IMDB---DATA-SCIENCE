# IMDB - DATA SCIENCE

![Top Votes](top_votes.png)

We daily come across movies and TV shows in our lives many of us actually prefer going online to movie review sites such as IMDB or Rotten Tomatoes to get some recommendations and insights about movies, TV shows etc. These websites are popular and have a huge collection of movies and TV show data. Each of the website provides some set of rating like IMDB score, Metascore and Tomatometer to present critic ratings. The main question that arises is that how we can use the data beyond what is readily available and add more sense to it

- Technologies: Python, Spark, MLlib, Cassandra, nltk
- Size of data scrapped: 13GB

### Files
- fetch_data.py: for web scrapping
- ETL/filter_functions.py:for filtering data
- ETL/movie_tv_tools.py: Schemas of the data
- ETL/imdb_etl.py: for cleaning and transformation of data
- load_data/load_imdb_data_cassandra.py: to load IMDB data into cluster
- load_data/load_RT_data_cassandra.py: to load Rotten Tomatoes data into cluster
- load_data/load_imdb_tools.py: file with schemas
- analysis1.py: analysis on data
- analysis2.py: more analysis on data
- imdb_topic_model.py: topic modeling of data
- imdb_word_cloud.py: word cloud generation based on genre
- pred_imdb.py: Prediction of IMDB score
- pred_imdb_test.py: Test the model on IMDB test data
- pred_rt.py: Prediction of critic average of rotten tomatoes data.

For running instructions open ![](RUNNING.md)

### Example Analysis on the data
![Top Votes](top_votes.png)
Analyzed which top 10 movies with the maximum number of votes
