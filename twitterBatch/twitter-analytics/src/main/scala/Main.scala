import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("hello world")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    //hashtagCounter(spark)
    // hashtagCounter(spark, "Trump".toLowerCase())
    hashtagCounter(spark, "Hillary".toLowerCase())
    spark.stop()
  }

  /** hashtagCounter
    *
    * @param spark
    */
  def hashtagCounter(spark: SparkSession) {
    import spark.implicits._
    //val staticDF = spark.read.json("/data-lake/*/*bz2").cache()
    val staticDF = spark.read.json("/test-data").cache()

    staticDF
      .filter(staticDF("entities.hashtags.text").isNotNull)
      .groupBy("entities.hashtags.text".toLowerCase())
      .count()
      .sort($"count".desc)
      .show(10, false)
  }

  def hashtagCounter(spark: SparkSession, hashtag: String) {
    import spark.implicits._
    // val staticDF = spark.read.json("/test-data").cache()
    val staticDF = spark.read.json("/data-lake/11-09-16/08").cache()

    val num = staticDF
      .filter(staticDF("entities.hashtags.text").isNotNull)
      .map(l => l.toString().toLowerCase())
      .flatMap(_.split(" "))
      .filter(_.startsWith("#" + hashtag))
      .count()

    println(s"Number of #$hashtag is " + num)
  }
}
