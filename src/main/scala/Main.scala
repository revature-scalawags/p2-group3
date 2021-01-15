import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("count hashtags")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    countHashtags(spark)
    spark.stop()

    def countHashtags(spark: SparkSession) {
      import spark.implicits._
      // val jsonfile = spark.read.option("multiline", "true").json("/datalake/00.json")
      val jsonfile = spark.read.json("/datalake").cache()
      // jsonfile.show()
      // jsonfile.printSchema()
      jsonfile
        .filter(jsonfile("entities.hashtags.text").isNotNull)
        .groupBy("entities.hashtags.text")
        .count()
        .sort($"count".desc)
        .show(100, false)

      //TODO:parse out each wrappedarray word
      //random fails..
      // val hashTags = jsonfile.select("entities.hashtags.text").collect().flatMap(_.getAs[mutable.WrappedArray[String]](0))
      // hashTags.foreach(println)
      //https://stackoverflow.com/questions/57346978/spark-split-is-not-a-member-of-org-apache-spark-sql-row

    }
  }
}
