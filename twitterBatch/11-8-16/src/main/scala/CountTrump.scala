import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources

object CountTrump {
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

    countHashtagsTrump(spark, inputFile)

    spark.stop()

    def countHashtagsTrump(spark: SparkSession, inputFile: String) {
      import spark.implicits._

      // jsonfile.show()
      // jsonfile.printSchema()

      //prints out total count of trump hashtags
      println("")
//https://stackoverflow.com/questions/44792616/how-to-convert-array-of-strings-to-string-column
      jsonfile
        .select($"entities.hashtags.text".as("trumpCount"))
        .withColumn("concatString", concat_ws(",", $"trumpCount"))
        .drop($"trumpCount")
        .groupBy($"concatString")
        .count()
        .filter(
          lower($"concatString").contains("trump") || lower($"concatString")
            .contains("donald")
        )
        .agg(sum($"count"))
        .show()
    }
  }
}
