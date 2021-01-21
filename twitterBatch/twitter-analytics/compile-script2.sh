#!/bin/bash
set -e
sbt assembly
docker cp target/scala-2.12/hello-world_2.12-1.0.jar spark-master:/tmp/hello-world.jar
docker exec spark-master bash -c "./spark/bin/spark-submit --class "TwitterStream" --master local[*] /tmp/hello-world.jar"