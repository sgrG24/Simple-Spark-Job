
resolvers ++= Seq(
  "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases"
)

lazy val sparkApp = (project in file("."))
  .settings(
    name := "SimpleSparkJob",
    version := "1.0.0-SNAPSHOT",
    scalaVersion := "2.12.11",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    libraryDependencies ++= Seq(scalatest, junit, sparkCore, sparkSql, scaldingArgs, hadoop_aws, awsSdkSTS,
      slf4jApi, spray_json)
  )

val sparkVersion = "2.4.4"

val scalatest = "org.scalatest" %% "scalatest" % "3.0.3" % Test
val junit = "junit" % "junit" % "4.11" % Test
val scaldingArgs = "com.twitter" %% "scalding-args" % "0.17.3" % "compile"
val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
val sparkSql = "org.apache.spark"  %% "spark-sql" % sparkVersion % "provided"

val awsSDKVersion = "1.11.749"

lazy val awsSdkSTS   = "com.amazonaws" % "aws-java-sdk-sts" % awsSDKVersion

val slf4jVersion = "1.7.25"

val slf4jApi         = "org.slf4j"                     % "slf4j-api"                      % slf4jVersion

val hadoop_aws = "org.apache.hadoop" % "hadoop-aws" % "2.6.0"

val spray_json = "io.spray" %%  "spray-json" % "1.3.5"