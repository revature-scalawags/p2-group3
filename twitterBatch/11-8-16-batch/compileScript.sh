#!/bin/bash
set -e

sbt package
docker cp target/scala-2.12/twitter-hashtags_2.12-1.0.jar spark-master:/tmp/twitter-hashtags.jar
docker exec spark-master bash -c "./spark/bin/spark-submit --class "Main" --master local[2] /tmp/twitter-hashtags.jar"