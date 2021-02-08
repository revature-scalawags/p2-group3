import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{ArrayType, StringType}

/** Use this to test the app locally
  * See compileScript.sh for details
  */
object HashtagsCountingLocalApp extends App {
  //val (inputFile, outputFile) = (args(0), args(1))

  val spark = SparkSession
    .builder()
    .appName("hashtag-counter")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  Runner.run(spark)
  // Runner.run(spark, inputFile, outputFile)
  spark.stop()
}
object Runner {

  def run(spark: SparkSession, inputFile: String, outputFile: String): Unit = {
    import spark.implicits._

    val hashtagSchema = loadHashtagSchema();
    // val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    // val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    // val s3Endpoint = "s3a://com.revature.scalawags.group3"

    // spark.sparkContext.hadoopConfiguration
    //   .set("fs.s3n.awsAccessKeyid", accessKeyId)
    // spark.sparkContext.hadoopConfiguration
    //   .set("fs.s3n.awsSecretAccessKey", secretAccessKey)

    val jsonDF =
      spark.read
        .format("json")
        .schema(hashtagSchema)
        .load(s"${inputFile}*")
        .select(explode($"entities.hashtags.text") as "hashtags")
        .cache()

    // val parqDF = spark.read.parquet(
    //   s"$s3Endpoint.datalake/top20hashtags.snappy.parquet"
    // )

    val sortedResults =
      getTopNHashtags(jsonDF, 20).write
        .format("parquet")
        .save(s"${outputFile}/topHashtags")
  }

  def getTopNHashtags(df: DataFrame, topN: Int): DataFrame = {
    df.groupBy("hashtags")
      .count()
      .sort(desc("count"))
      .limit(topN)
  }

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
