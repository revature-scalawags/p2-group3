#!/bin/bash
set -e

sbt package
docker cp target/scala-2.12/twitter-hashtags_2.12-1.0.jar spark-master:/tmp/twitter-hashtags.jar
docker exec spark-master bash -c "./spark/bin/spark-submit --class "ShowTrump" --master local[2] /tmp/twitter-hashtags.jar /datalake/"

# to test just one folder uncomment line below
# docker exec spark-master bash -c "./spark/bin/spark-submit --class "ShowTrump" --master local[2] /tmp/twitter-hashtags.jar /datalake/00/"
# should be able to increase memory with --driver-memory 1g before --class