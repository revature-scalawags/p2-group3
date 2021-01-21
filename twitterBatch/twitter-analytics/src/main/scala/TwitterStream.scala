import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object TwitterStream {
  def main(args: Array[String]) {
    setupTwitter()
    setupLogging()

    val spark = new StreamingContext("local[*]", "Hashtags", Seconds(1))
    val tweets = TwitterUtils.createStream(spark, None)
    val statuses = tweets.map(status => status.getText)
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    val hashtags = tweetwords.filter(word => word.startsWith("#"))
    val hashtagKey = hashtags
      .map(hashtag => hashtag.toLowerCase())
      .filter(hashtag => hashtag.contains("#trump") || hashtag.contains("#biden"))
      .map(hashtag => (hashtag, 1))

    val hashtagCounts = hashtagKey.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    val count = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    count.print

    spark.checkpoint("C:/checkpoint/")
    spark.start()
    spark.awaitTermination()
}

  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }
  def setupTwitter(): Unit = {
    import scala.io.Source

    val fileName = "twitter.txt"
    val lines = Source.fromFile(fileName)
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
    lines.close()
  }

}