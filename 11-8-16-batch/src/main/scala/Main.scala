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
      // val jsonfile = spark.read.json("/datalake/00/*bz2").cache()
      // jsonfile.show()
      // jsonfile.printSchema()

      //works cited: https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala
      //prints out total count of trump hashtags
      println("")
      // println("Total number of Donald Trump related hashtags")
      // val countTrump: Unit = jsonfile
      //   .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
      //   .select($"_tmp".getItem(0).as("col1"))
      //   .groupBy(("col1"))
      //   .count()
      //   .filter(lower($"col1") === "trump" || lower($"col1") === "donald")
      //   .agg(sum($"count"))
      //   .show()

      //prints out count of hillary hashtags of all casing
      println("Total number of Hillary Clinton related hashtags")
      val countClinton = jsonfile
        .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
        .select($"_tmp".getItem(0).as("col1"))
        .groupBy(("col1"))
        .count()
        .filter(lower($"col1") === "hillary" || lower($"col1") === "clinton")
        .agg(sum($"count"))
        .show()

      jsonfile
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

//prints out inidivdual counts
// val countClinton = jsonfile
//   .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
//   .select($"_tmp".getItem(0).as("col1"))
//   .groupBy(("col1"))
//   .count()
//   .sort($"count".desc)
//   .filter(lower($"col1") === "hillary" || lower($"col1") === "clinton")
//   .show()

