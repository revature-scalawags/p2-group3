# Twitter Analytics

## Program that compares political leader mentions on Twitter using historical and streaming data

## Technologies

- Spark SQL version 3.0.0
- Spark Core version 3.0.0
- Scala version 2.12.10
- SBT version 1.4.4

## Features

- Historical Batch Twitter Analysis
  - 11-06-16: Compares twitter replies
  - 11-07-16: Counts top hashtags of prior to election day
  - 11-08-16 to 11-09-16: Compares Donald Trump vs Hillary Clinton hashtag mentions
- Streaming Data Analysis
  - Counts Joe Biden vs Donald Trump hashtag mentions
- To-do list:
  - Create AWS S3 pipeline
  - Execute program on AWS EMR cluster

## Getting Started

- Clone repository:

  > git clone https://github.com/revature-scalawags/p2-group3.git

- Spark cluster setup on docker: (optional):  
  https://github.com/big-data-europe/docker-spark
- Historical Twitter Data to analyze (example):  
  https://archive.org/details/archiveteam-twitter-stream-2016-11
- Twitter API developer account (for streaming analysis)
- Twitter API keys

## Usage

For historical batch analysis:

- run scripts in individual date folders

For streaming results:

> sbt compile  
> sbt run

## References

[Historical Twitter Dataset](https://archive.org/details/archiveteam-twitter-stream-2016-11)  
[Big Data Europe's standalone Apache Spark Cluster](https://github.com/big-data-europe/docker-spark)

## Contributors

- [Myung Jin Song](https://github.com/jsong220)
- [Michael Tsoumpariotis](https://github.com/MichaelT950)
- [Bryant Singh](https://github.com/brysingh76)
