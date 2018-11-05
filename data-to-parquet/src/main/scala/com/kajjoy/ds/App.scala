package com.kajjoy.ds

import com.amazonaws.regions.Regions
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Minutes, StreamingContext, Time}
import org.apache.spark.streaming.kinesis._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kinesis.KinesisInitialPositions.TrimHorizon
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType

/**
  * @author Dipayan Chattopadhyay
  */
object App extends BootstrapTrait with HelperTrait {

  def main(args: Array[String]) {

    val deploymentEnvironment: String = Config.getDeploymentEnvironment

    if (deploymentEnvironment.isEmpty) {
      print("Deployment Environment not specified, exiting application")
      throw new IllegalAccessException("Deployment Environment is mandatory, needs to be provided")
    }

    println("Running" + applicationName + "Application in" + deploymentEnvironment + " environment")
    implicit val spark: SparkSession = initializeSpark(deploymentEnvironment)
    loadProps(spark)

    val numShards = getNumberOfkinesisShards(kinesisStreamName, assumeRoleArn, region)
    val batchSize = Minutes(timeIntervalInMinutes)
    val streamingContext: StreamingContext = new StreamingContext(spark.sparkContext, batchSize)

    val numStreams = numShards
    System.out.println("Number of shards: " + numShards)


    val kinesisStreams = (0 until numStreams).map { stream =>
      val builder =
        KinesisInputDStream.builder
          .streamingContext(streamingContext)
          .endpointUrl(constructKinesisEndpointUrl(region = region))
          .regionName(Regions.fromName(region).getName)
          .streamName(kinesisStreamName)
          .initialPosition(new TrimHorizon())
          .checkpointAppName(applicationName)
          .checkpointInterval(batchSize)

      if (assumeRoleArn.isEmpty) {
        builder.build()
      }
      else {
        builder.kinesisCredentials(getSTSCredentialForSparkStreaming(assumeRoleArn))
          .dynamoDBCredentials(getSTSCredentialForSparkStreaming(assumeRoleArn))
          .cloudWatchCredentials(getSTSCredentialForSparkStreaming(assumeRoleArn))
          .build()
      }

    }

    val schema: StructType = spark.read.json(sampleRecord).schema

    // Union all the streams
    val unionStreams: DStream[Array[Byte]] = streamingContext.union(kinesisStreams)

    unionStreams.repartition(10)
    unionStreams.foreachRDD((rdd: RDD[Array[Byte]], time: Time) => {
      if (!rdd.isEmpty()) {
        val df : DataFrame =convertByteArrayToDataFrame(rdd, schema)
        writeToParquet(df)
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}