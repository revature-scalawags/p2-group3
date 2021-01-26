import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShowClinton {
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

    showHashtagsClinton(spark, inputFile)

    spark.stop()

    def showHashtagsClinton(spark: SparkSession, inputFile: String) {
      import spark.implicits._

      println("")
      //prints out count of contents hashtags of all casing
      println("Top 20 mentions of Clinton")
      jsonfile
        .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
        .select($"_tmp".getItem(0).as("clintonCount"))
        .groupBy(("clintonCount"))
        .count()
        .sort($"count".desc)
        .filter(
          lower($"clintonCount").contains("hillary") || lower($"clintonCount")
            .contains("clinton")
        )
        .show()
    }

  }
}
