import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

//References:tw "Apache Spark with Scala - Hands On with Big Data!" Course by Frank Kane 
//http://twitter4j.org/en/code-examples.html
//https://stackoverflow.com/questions/9997292/how-to-read-environment-variables-in-scala
object PopularHashtags {
  
    /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}   
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)   
  }
  
  /** Configures Twitter service credentials with environment variables */
  def setupTwitter(): Unit = {
    val consumer_Key = sys.env("twitterConsumerKey")
    val consumer_Secret = sys.env("twitterConsumerSecret")
    val access_Token = sys.env("twitterAccessToken")
    val access_Secret = sys.env("twitterAccessSecret")

    System.setProperty("twitter4j.oauth.consumerKey", consumer_Key)
    System.setProperty("twitter4j.oauth.consumerSecret", consumer_Secret)
    System.setProperty("twitter4j.oauth.accessToken", access_Token)
    System.setProperty("twitter4j.oauth.accessTokenSecret", access_Secret)
  }

  def main(args: Array[String]) {

    setupTwitter()

    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    // Get rid of log spam
    setupLogging()

    // Create a DStream from Twitter using our streaming context that returns tweets
    // Twitter API limits random 1% of total real time tweets
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText)
    // Separate out each word
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
    // Now eliminate anything that's not a hashtag and handle non-spaced hashtags
    val hashtags = tweetwords.filter(word => word.startsWith("#")).flatMap(tweetText => tweetText.split("#"))
    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    //maybe exclude donald and joe as they are more common first names
    val hashtagKeyLowercase = hashtags
      .map(hashtag => hashtag.toLowerCase())
      .filter(hashtag => hashtag.contains("donald") || 
        hashtag.contains("trump") || 
        hashtag.contains("joe") || 
        hashtag.contains("biden"))
      .map(hashtag => (hashtag, 1))
    
    // Reduce last 30 seconds of data, every second
    val hashtagCounts = hashtagKeyLowercase.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x + y, Seconds(30), Seconds(1))
    // Sort based on count
    val countResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))
    
    // Print results
    countResults.print
    
    // Set a checkpoint directory for reduceByKeyAndWindow functionality
    // filling usage of stateful transformations requirement 
    // change checkpoint directory based on os file system
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}