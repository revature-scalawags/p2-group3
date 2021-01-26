# Project 2 - Comparing Trump and Clinton mentions

# Dependencies
> using https://github.com/big-data-europe/docker-spark

## Run
>1. Copy bz2 twitter archive files into docker: spark-master:/datalake/
>- Retreived from https://archive.org/details/archiveteam-twitter-stream-2016-11
>2. Four different method queries available, run individual bash scripts for desired queries
- To show total count and individual hashtag mentions related to Donald Trump: 
    - ./countTrump.sh and ./showTrump.sh 
- To show total count and individual hashtag mentions related to Hillary Clinton:
    - ./countClinton.sh and ./showClinton.sh
- To run all queries
    - ./compileAll.sh
    - **this may take a long time to finish all results


# Features
- Counts total number of hashtag occurances for date 11-8-2016
    - Counts all Donald Trump tweets
    - Counts all Hillary Clinton tweets
- Shows contents of all hashtag occurances by different casing 

# Technologies
- Apache Spark
- Spark SQL
- YARN
- Scala
- S3
- Git with Github
- Well documented 