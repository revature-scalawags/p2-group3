import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
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
    // showHashtagsTrump(spark, inputFile)
    // countHashtagsClinton(spark, inputFile)
    // showHashtagsClinton(spark, inputFile)

    spark.stop()

    def countHashtagsTrump(spark: SparkSession, inputFile: String) {
      import spark.implicits._

      // jsonfile.show()
      // jsonfile.printSchema()

      //works cited: https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala
      //prints out total count of trump hashtags
      println("")
      println("Total number of Donald Trump related hashtags")
      val countTrump: Unit = jsonfile
        .withColumn("_tmp", split($"entities.hashtags.text".getItem(0), "\\,"))
        .select($"_tmp".getItem(0).as("trumpCount"))
        .groupBy(("trumpCount"))
        .count()
        .filter(
          lower($"trumpCount").contains("trump") || lower($"trumpCount")
            .contains("donald")
        )
        .agg(sum($"count"))
        .show()
    }

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
