import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{ArrayType, StringType}

/** Use this to test the app locally
  * See compileScript.sh for details
  */
object HashtagsCountingLocalApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))

  val spark = SparkSession
    .builder()
    .appName("hashtag-counter")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  Runner.run(spark, inputFile, outputFile)
  spark.stop()
}

/** Use this when submitting the app to a cluster with spark-submit
  */
object HashtagsCountingApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  val spark =
    SparkSession.builder.appName("twitter-hashtag-counter").getOrCreate();
  Runner.run(spark, inputFile, outputFile)
}

object Runner {
  def run(spark: SparkSession, inputFile: String, outputFile: String): Unit = {
    import spark.implicits._
    val hashtagSchema = loadHashtagSchema();
    val jsonDF =
      spark.read
        .format("json")
        .schema(hashtagSchema)
        .load(s"${inputFile}*")
        .select(explode($"entities.hashtags.text") as "hashtags")
        .cache()
    // .write
    // .format("parquet")
    // .save(s"${outputFile}")

    val sortedResults =
      getTopHashtags(jsonDF).write.format("parquet").save(s"${outputFile}")

    // val parquet =
    //   spark.read
    //     .format("parquet")
    //     .load(s"${inputFile}*")
    //     .sort(desc("count"))
    //     .write
    //     .format("csv")
    //     .save(s"${outputFile}")
  }

  def getTopHashtags(df: DataFrame): DataFrame = {
    df.groupBy("hashtags")
      .count()
      .sort(desc("count"))
  }

  // def searchHashtag(hashtag: String, df: DataFrame): Seq[String] {

  // }

  def loadHashtagSchema(): StructType = {
    val hashtag = StructType(Array(StructField("text", StringType, false)))
    val customSchema = StructType(
      Array(
        StructField(
          "entities",
          StructType(
            Array(StructField("hashtags", ArrayType(hashtag, false), false))
          ),
          false
        )
      )
    )
    customSchema
  }

}
