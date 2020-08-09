import com.amazonaws.auth.{InstanceProfileCredentialsProvider, STSAssumeRoleSessionCredentialsProvider}
import com.amazonaws.regions.Regions
import com.amazonaws.services.securitytoken.model.{AssumeRoleRequest, GetCallerIdentityRequest}
import com.amazonaws.services.securitytoken.{AWSSecurityTokenService, AWSSecurityTokenServiceClientBuilder}
import com.amazonaws.util.EC2MetadataUtils
import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

class SparkJob {
  def solve(inputDF: DataFrame) = {
    val outputDF = inputDF.where(col("Id") === "2")
    println(s"Number Of Objects: ${outputDF.count()}")
    outputDF
  }
}


object SparkJob {

  def assumeIamRole(configuration: Configuration,
                    stsClient: AWSSecurityTokenService,
                    roleARN: String,
                    roleSessionName: String
                        ) = {
    println("CALLING Assuming Role")

    configuration.set("fs.s3a.aws.credentials.provider", "CustomCredentialProvider")
    configuration.set("fs.s3a.assumed.role.arn", roleARN)
  }

  def setInstanceCredentialProvider(configuration: Configuration) = {
    configuration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
  }

  def main(args: Array[String]): Unit = {
    val cmdArgs = Args(args)
    val input_path = cmdArgs("input_file")
    val output_path = cmdArgs("output_file")
    val roleArnRead = cmdArgs.getOrElse("roleArnRead", "None")
    val roleArnWrite = cmdArgs.getOrElse("roleArnWrite", "None")

    val clientRegion = Regions.US_EAST_1


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Spark Job")
      .getOrCreate()

    println(s"Input file: ${input_path}, Output file: ${output_path}")

    println("Loading STS Temporary credentials....")
    val roleSessionName: String = EC2MetadataUtils.getInstanceId() + "Session"

    val stsClient: AWSSecurityTokenService = AWSSecurityTokenServiceClientBuilder
      .standard()
      .withRegion(clientRegion)
      .build()

    val configuration = sparkSession.sparkContext.hadoopConfiguration
    //Important setting: Without this, codes failed.

    println("Printing Hadoop Configs before assuming read role: " + configuration)
    if(roleArnRead != "None") assumeIamRole(configuration, stsClient, roleArnRead, roleSessionName)
    println("Printing Hadoop Configs after assuming read role: " + configuration.toString)


    println("Fetching input data from S3 bucket");
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

    setInstanceCredentialProvider(configuration)
    println("Printing Hadoop Configs before assuming write role: " + configuration.toString)
    if(roleArnWrite != "None") assumeIamRole(configuration, stsClient, roleArnWrite, roleSessionName)
    println("Printing Hadoop Configs after assuming write role: " + configuration.toString)

    println("Storing output data back to S3 bucket")
    outputDF.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .save(output_path)

    println(String.format("Exiting application, byee...."))

  }

}
