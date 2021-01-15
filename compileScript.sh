#!/bin/bash
set -e

# export TWITTER_CONSUMER_TOKEN_KEY='my-consumer-key'
# export TWITTER_CONSUMER_TOKEN_SECRET='my-consumer-secret'
# export TWITTER_ACCESS_TOKEN_KEY='my-access-key'
# export TWITTER_ACCESS_TOKEN_SECRET='my-access-secret'

sbt package
docker cp target/scala-2.12/twitter-hashtags_2.12-1.0.jar spark-master:/tmp/twitter-hashtags.jar
docker exec spark-master bash -c "./spark/bin/spark-submit --class "Main" --master local[*] /tmp/twitter-hashtags.jar"