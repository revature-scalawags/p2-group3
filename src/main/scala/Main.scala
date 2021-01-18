import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("count hashtags")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    countHashtags(spark)
    spark.stop()

    def countHashtags(spark: SparkSession) {
      import spark.implicits._
      // val jsonfile = spark.read.option("multiline", "true").json("/datalake/00.json")
      val jsonfile = spark.read.json("/datalake/*/*bz2").cache()
      // jsonfile.show()
      // jsonfile.printSchema()

      //works cited: https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala
      //prints out total count of trump hashtags
      val countTrump: Unit = jsonfile
        .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
        .select($"_tmp".getItem(0).as("col1"))
        .groupBy(("col1"))
        .count()
        .filter(lower($"col1") === "trump" || lower($"col1") === "donald")
        .agg(sum($"count"))
        .show()

      //prints out count of hillary hashtags of all casing
      val countClinton = jsonfile
        .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
        .select($"_tmp".getItem(0).as("col1"))
        .groupBy(("col1"))
        .count()
        .sort($"count".desc)
        .filter(lower($"col1") === "hillary" || lower($"col1") === "clinton")
        .show()

    }
  }
}

// case class mySchema(
//   contributors: String,
//   coordinates: String,
//   created_at: String,
//   delete: String,
//   display_text_range: String,
//   entities: Array[String],
//   extended_entities: Array[String],
//   extended_tweet: String,
//   favorite_count: Int,
//   favorited: Boolean,
//   filter_level: String,
//   geo: String,
//   id: BigInt,
//   id_str: BigInt,
//   in_reply_to_screen_name: String,
//   in_reply_to_status_id: BigInt,
//   in_reply_to_status_id_str: BigInt,
//   in_reply_to_user_id: BigInt,
//   in_reply_to_user_id_str: BigInt,
//   is_quote_status: Boolean,
//   lang: String,
//   place: String,
//   possibly_sensitive: Boolean,
//   quote_count: Int,
//   quoted_status: String,
//   quoted_status_id: Int,
//   quoted_status_id_str: BigInt,
//   quoted_status_permalink: Array[String],
//   reply_count: Int,
//   retweet_count: Int,
//   retweeted: Boolean,
//   retweeted_status: Array[String],
//   source: String,
//   text: String,
//   timestamp_ms: BigInt,
//   truncated: Boolean,
//   user: Array[String])
