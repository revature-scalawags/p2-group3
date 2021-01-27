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
        .select($"entities.hashtags.text".as("trumpCount"))
        .withColumn("Trump Mentions", concat_ws(",", $"trumpCount"))
        .drop($"trumpCount")
        .groupBy($"Trump Mentions")
        .count()
        .sort($"count".desc)
        .filter(
          lower($"Trump Mentions").contains("trump") || lower($"Trump Mentions")
            .contains("donald")
        )
        .show(false)
    }
  }
}
