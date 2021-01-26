import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShowTrump {
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

    showHashtagsTrump(spark, inputFile)

    spark.stop()

    def showHashtagsTrump(spark: SparkSession, inputFile: String) {
      import spark.implicits._

      println("")
      //prints out contents of Trump hashtags of all casing
      println("Top 20 mentions of Trump")
      jsonfile
        .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
        .select($"_tmp".getItem(0).as("trumpCount"))
        .groupBy(("trumpCount"))
        .count()
        .sort($"count".desc)
        .filter(
          lower($"trumpCount").contains("trump") || lower($"trumpCount")
            .contains("donald")
        )
        .show()
    }
  }
}
