#!/bin/bash
set -e
sbt package

docker cp target/scala-2.12/top-hashtags_2.12-1.0.jar spark-master:/tmp/top-hashtags_2.12-1.0.jar
docker exec spark-master bash -c "./spark/bin/spark-submit --class "HashtagsCountingLocalApp" --master local[*] /tmp/top-hashtags_2.12-1.0.jar /datalake/11-07-16/ /datawarehouse/results/" 