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
      jsonfile
        .select($"entities.hashtags.text".as("clintonCount"))
        .withColumn("concatString", concat_ws(",", $"clintonCount"))
        .drop($"clintonCount")
        .groupBy($"concatString")
        .count()
        .filter(
          lower($"concatString").contains("hillary") || lower($"concatString")
            .contains("clinton")
        )
        .agg(sum($"count"))
        .show()
    }
  }
}
