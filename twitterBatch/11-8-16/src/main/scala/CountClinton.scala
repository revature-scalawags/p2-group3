import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CountClinton {
  def main(args: Array[String]) {

    val inputFile = args(0)

    val spark = SparkSession
      .builder()
      .appName("count hashtags")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val jsonfile =
      // val jsonfile = spark.read.json("/datalake/00/*bz2").cache()
      spark.read.option("recursiveFileLookup", true).json(s"$inputFile").cache()

    countHashtagsClinton(spark, inputFile)

    spark.stop()

    def countHashtagsClinton(spark: SparkSession, inputFile: String) {
      import spark.implicits._

      println("")
      //prints out total count of Clinton hashtags
      println("Total number of Hillary Clinton related hashtags")
      val countClinton = jsonfile
        .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
        .select($"_tmp".getItem(0).as("clintonCount"))
        .groupBy(("clintonCount"))
        .count()
        .filter(
          lower($"clintonCount").contains("hillary") || lower($"clintonCount")
            .contains("clinton")
        )
        .agg(sum($"count"))
        .show()
    }
  }
}
