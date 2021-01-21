
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("counts number of people who replied to Trump's tweets")
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    countTweetRT(spark)
    spark.stop()

    def countTweetRT(spark: SparkSession) {
      import spark.implicits._
      val jsonfile = spark.read.json("C:/Users/bryan/Downloads/11-06-16-20210118T214424Z-001/11-06-16/*")
      
      //val jsonfile = spark.read.json("/datalake/*/*bz2").cache()
      jsonfile.printSchema()

      println("Counts the number of people who replied to Trump's tweets")
      jsonfile.groupBy("in_reply_to_screen_name")
        .count()
        .filter($"in_reply_to_screen_name" === "realDonaldTrump")
        .agg(sum($"count")).show()

      println("Counts the number of people who replied to Clinton's tweets")
      jsonfile.groupBy("in_reply_to_screen_name")
        .count()
        .filter($"in_reply_to_screen_name" === "HillaryClinton")
        .agg(sum($"count")).show()

      // println("Counts the number of times Hillary Clinton tweeted")
      // jsonfile.groupBy("user.screen_name")
      //   .count()
      //   .filter($"user.screen_name" === "JoeBiden")
      //   .agg(sum($"count")).show()
    }
  }
}

