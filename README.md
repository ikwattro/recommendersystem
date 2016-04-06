Recommender system demo code
----------------------------

## Installation
Before you use this demo, please make sure that you have Hadoop, Spark and Cassandra up and running.
You can download both from the internet:

- Spark: spark.apache.org
- Hadoop: hadoop.apache.org
- Cassandra: cassandra.apache.org

## Quick start
After you have Spark, Hadoop and cassandra installed run the following scripts on the Cassandra server using CQLSH
in order to get the database created and filled with sample data:

```
create keyspace recommendations with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
create table recommendations.user_item_rating (
    user_id int,
    item_id int,
    rating double,
    primary key(user_id, item_id)
);
```

After you created the database, create a CSV file with the following content:
user_id, item_id, rating where each is an integer value. Load this new file into Cassandra using the command below:

```
copy user_item_rating (user_id, item_id, rating) from 'user_item_ratings.csv' with HEADER=true;
```

Next start the application using the follow command:

```
sbt run
```

Finally use CURL to train the first version of the recommender system model

```
curl -XPOST http://localhost:8080/train
```

Enjoy the show. You can get recommendations by opening `http://localhost:8080/recommendations/[id]` in a browser.
