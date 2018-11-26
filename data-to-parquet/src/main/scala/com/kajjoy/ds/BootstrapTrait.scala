package com.kajjoy.ds

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kinesis.SparkAWSCredentials

trait BootstrapTrait {

  var applicationName: String = Config.getAppName
  var outputPath: String = _
  var kinesisStreamName : String = _
  var region : String = _
  var timeIntervalInMinutes : Int = 5
  var assumeRoleArn : String = _
  var inputDataFormat : DataFormat.Value = _
  var outputDataFormat : DataFormat.Value = _
  var sampleRecord : String = _

  def initializeSpark(deploymentEnvironment: String): SparkSession = {
    assert(applicationName != null)
    if (deploymentEnvironment.equals("local")) {
      val sparkSession: SparkSession = SparkSession
        .builder()
        .appName(applicationName)
        .master("local[*]")
        .getOrCreate()

      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3-us-west-2.amazonaws.com")

      setSparkProperties(sparkSession)

      sparkSession
    } else {
      val sc = SparkSession
        .builder()
        .appName(applicationName)
        .getOrCreate()
      setSparkProperties(sc)
      sc
    }
  }

  def loadProps(spark: SparkSession): Unit = {
    outputPath = spark.conf.get("spark.output.path", Config.getOutPutS3Path)
    kinesisStreamName = spark.conf.get("spark.kinesis.stream.name")
    assumeRoleArn = spark.conf.get("spark.kinesis.assume.role.arn")
    region = spark.conf.get("spark.aws.region")
    sampleRecord = spark.conf.get("spark.input.record.sample")
    inputDataFormat = getDataFormat(spark.conf.get("spark.input.record.format"))
    outputDataFormat = getDataFormat(spark.conf.get("spark.output.record.format"))

    try {
      timeIntervalInMinutes = spark.conf.get("spark.batch.interval.time.in.minutes").toInt
    } catch {
      case e : ClassCastException => throw new IllegalArgumentException("timeIntervalInMinutes needs to be an Integer value wrapped up in String")
    }

    println(s"Loaded properties are:  s3OutputPath= $outputPath, kinesisStreamName= $kinesisStreamName, applicationName = $applicationName" +
      s", assumeRoleArn = $assumeRoleArn, timeIntervalInMinutes = $timeIntervalInMinutes, region= $region, sampleRecord= $sampleRecord, " +
      s"inputDataFormat = $inputDataFormat, outputDataFormat = $outputDataFormat")
  }

  private def setSparkProperties(sc: SparkSession): Unit = {
    sc.sparkContext.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    sc.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload", "true")
    sc.sparkContext.hadoopConfiguration.set("fs.s3a.acl.default", "AuthenticatedRead")
    sc.conf.set("spark.speculation", "false")
    sc.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    sc.sqlContext.setConf("spark.sql.parquet.mergeSchema", "false")
    sc.sqlContext.setConf("spark.sql.parquet.filterPushdown", "true")
  }

  def getSTSAssumeRoleCredentials(stsRole: String) : STSAssumeRoleSessionCredentialsProvider  = {
    new STSAssumeRoleSessionCredentialsProvider.Builder(stsRole, RandomStringUtils.randomAlphanumeric(8))
      .build()
  }

  def getSTSCredentialForSparkStreaming(stsRole : String) = {
    SparkAWSCredentials.builder.stsCredentials(stsRole, RandomStringUtils.randomAlphanumeric(8)).build()
  }

  def getAWSKinesisClientWithAssumeRole(assumeRole : String, region : String): AmazonKinesis = {
    AmazonKinesisClientBuilder
      .standard()
      .withRegion(Regions.fromName(region))
      .withCredentials(getSTSAssumeRoleCredentials(assumeRole))
      .build()
  }

  def getAWSKinesisClient(region : String) : AmazonKinesis = {
    AmazonKinesisClientBuilder
      .standard()
      .withRegion(Regions.fromName(region))
      .build()
  }

  def constructKinesisEndpointUrl(region : String) : String = {
    s"kinesis.${Regions.fromName(region).getName}.amazonaws.com"
  }

  def getNumberOfkinesisShards(streamName: String, assumeRole : String, region : String) : Int = {
    var amazonKinesisClient : AmazonKinesis = getAWSKinesisClient(region)
    if(! assumeRole.isEmpty){
      amazonKinesisClient = getAWSKinesisClientWithAssumeRole(assumeRole = assumeRole, region = region)
    }
    amazonKinesisClient.describeStream(streamName).getStreamDescription.getShards.size
  }

  def getDataFormat(formatString : String) : DataFormat.Value = {
    formatString match {
      case u if u.equals(DataFormat.JSON.toString) => DataFormat.JSON
      case u if u.equals(DataFormat.PARQUET.toString) => DataFormat.PARQUET
      case u if u.equals(DataFormat.CSV.toString) => DataFormat.CSV
      case _ => throw new IllegalArgumentException("Only JSON | PARQUET | CSV formats are supported. No other formats are supported yet.")
    }
  }

}
