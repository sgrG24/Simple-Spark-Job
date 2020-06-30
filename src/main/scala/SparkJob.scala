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

class SparkJob{
  def solve(inputDF:DataFrame) = {
    val outputDF = inputDF.where(col("Id") === "2")
    println(s"Number Of Objects: ${outputDF.count()}")
    outputDF
  }
}


object SparkJob {

  def configureSecretKey(configuration: Configuration,
                         stsClient: AWSSecurityTokenService,
                         roleARN: String,
                         roleSessionName: String
                        ) = {
    val provider = new STSAssumeRoleSessionCredentialsProvider.Builder(
      roleARN, roleSessionName).withStsClient(stsClient).build()

    provider.getCredentials.getSessionToken
    provider.getCredentials.getAWSSecretKey
    provider.getCredentials.getAWSAccessKeyId
    val request = new AssumeRoleRequest()
    println(s"CURRENT ARN :: ${request.getRoleArn}")

    request.setRoleArn(roleARN)
    request.setRoleSessionName(roleSessionName)

    println(s"ASSUMING ARN :: ${request.getRoleArn}")

    println(s"Caller Identity Before assuming Role: ${stsClient.getCallerIdentity(new GetCallerIdentityRequest)}")

    val assumeRoleResult = stsClient.assumeRole(request)

    println(s"ASSUMED USER :: ${assumeRoleResult.getAssumedRoleUser}")

    println(s"ACCESS KEY: ${assumeRoleResult.getCredentials.getAccessKeyId}")
    println(s"SECRET KEY: ${assumeRoleResult.getCredentials.getSecretAccessKey}")
    println(s"SESSION TOKEN: ${assumeRoleResult.getCredentials.getSessionToken}")



    configuration.set("fs.s3a.access.key", assumeRoleResult.getCredentials().getAccessKeyId())
    configuration.set("fs.s3a.secret.key", assumeRoleResult.getCredentials().getSecretAccessKey())
    configuration.set("fs.s3a.session.token", assumeRoleResult.getCredentials().getSessionToken())

    println(s"Caller Identity After assuming Role: ${stsClient.getCallerIdentity(new GetCallerIdentityRequest)}")
  }

  def main(args: Array[String]): Unit = {
    val cmdArgs = Args(args)
    val input_path = cmdArgs("input_file")
    val output_path = cmdArgs("output_file")
    val roleArnRead = cmdArgs("roleArnRead")
    val roleArnWrite =  cmdArgs("roleArnWrite")

    val clientRegion = Regions.US_EAST_1


    val sparkSession = SparkSession.builder()
      .appName("Spark Job")
      .getOrCreate()

    println(s"Input file: ${input_path}, Output file: ${output_path}")

    println("Loading STS Temporary credentials....")
    val roleSessionName: String = EC2MetadataUtils.getInstanceId() + "Session"

    val credentialsProvider = new InstanceProfileCredentialsProvider(true)

    val stsClient: AWSSecurityTokenService = AWSSecurityTokenServiceClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withRegion(clientRegion)
      .build()

    val configuration = sparkSession.sparkContext.hadoopConfiguration
    //Important setting: Without this, codes failed.
    configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

    println("Calling configure secret key")
    configureSecretKey(configuration, stsClient, roleArnRead, roleSessionName)

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

    configureSecretKey(configuration, stsClient, roleArnWrite, roleSessionName)
    println("Storing output data back to S3 bucket")
    outputDF.write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .save(output_path)

    println(String.format("Exiting application, byee...."))

  }

}
