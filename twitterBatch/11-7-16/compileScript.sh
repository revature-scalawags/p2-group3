#!/bin/bash
set -e
sbt package

#create seperate file "keys.sh" with env vars to store aws keys
#create both env vars in file
#example: export AWS_ACCESS_KEY_ID=yourkeyhere
#source keys.sh

# 1)download two jar files: 
# a) aws-java-sdk-bundle-1.11.375.jar
#    - https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.11.375
# b) hadoop-aws-3.2.0.jar
#    - https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.2.0

# 2)create lib folder in root directory of project and put both jars inside

# this copies the jar files into spark/jars folder
docker cp lib/aws-java-sdk-bundle-1.11.375.jar spark-master:/spark/jars/aws-java-sdk-bundle-1.11.375.jar
docker cp lib/hadoop-aws-3.2.0.jar spark-master:/spark/jars/hadoop-aws-3.2.0.jar

docker cp target/scala-2.12/batch-top-hashtags_2.12-0.1.jar spark-master:/jarFiles/batch-top-hashtags_2.12-0.1.jar
docker exec -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY spark-master bash -c "./spark/bin/spark-submit --class "HashtagsCountingLocalApp" --master local[*] /jarFiles/batch-top-hashtags_2.12-0.1.jar" 

# docker cp target/scala-2.12/top-hashtags_2.12-1.0.jar spark-master:/tmp/top-hashtags_2.12-1.0.jar
# docker exec spark-master bash -c "./spark/bin/spark-submit --class "HashtagsCountingLocalApp" --master local[*] /jarFiles/batch-top-hashtags_2.12-0.1.jar /datalake/11-07-16/ /datawarehouse/results/" 
