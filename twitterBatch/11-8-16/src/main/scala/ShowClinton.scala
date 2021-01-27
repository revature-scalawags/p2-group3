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
        .select($"entities.hashtags.text".as("clintonCount"))
        .withColumn("Clinton Mentions", concat_ws(",", $"clintonCount"))
        .drop($"clintonCount")
        .groupBy($"Clinton Mentions")
        .count()
        .sort($"count".desc)
        .filter(
          lower($"Clinton Mentions").contains("hillary") || lower($"Clinton Mentions")
            .contains("clinton")
        )
        .show(false)
    }

  }
}
