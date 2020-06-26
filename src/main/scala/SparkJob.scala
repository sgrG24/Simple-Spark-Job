import com.twitter.scalding.Args
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

class SparkJob{
  def solve(inputDF:DataFrame) = {
    val outputDF = inputDF.where(col("Id") === "2")
    outputDF
  }
}


object SparkJob {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val cmdArgs = Args(args)
    val input_path = cmdArgs("input_file")
    val output_path = cmdArgs("output_file")

    logger.info(s"Input file: ${input_path}, Output file: ${output_path}")

    val sparkSession = SparkSession.builder()
      .appName("Spark Job")
      .getOrCreate()

    logger.info("Fetching input data from S3 bucket");
    val schema = StructType(Array(
      StructField("Id", StringType),
      StructField("Item", StringType)
    ))

    val inputDF = sparkSession.read
      .schema(schema)
      .option("header", "true")
      .option("sep", ",")
      .csv(input_path)
    val outputDF = new SparkJob().solve(inputDF)

    logger.trace("Storing output data back to S3 bucket")
    outputDF.write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save(output_path)

    logger.info(String.format("Exiting application, byee...."))

  }

}
