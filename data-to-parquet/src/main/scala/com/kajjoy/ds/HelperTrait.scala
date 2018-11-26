package com.kajjoy.ds

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import com.kajjoy.ds.util.DateUtil
import org.apache.spark.sql._

trait HelperTrait {

  val YEAR_COL_NAME = "year"
  val MONTH_COL_NAME = "month"
  val DAY_COL_NAME = "day"

  /**
    * Convertes incoming raw data in byte array in a stream and converts it to the native data format specified.
    * @param rdd An rdd of byte arrays received from the raw stream.
    * @param tableSchema Schema of the data derived from the JSON provided as input for infering schema.
    * @param inputDataFormat Input data format for data in the stream. Currently data in csv and json are supported.
    * @param spark Spark context.
    * @return
    */
  def convertByteArrayToDataFrame(rdd: RDD[Array[Byte]], tableSchema : StructType, inputDataFormat : DataFormat.Value)(implicit spark: SparkSession): DataFrame  = {
    import spark.implicits._
    val dataSet : Dataset[String] = spark.createDataset(rdd.map(w => {
      w.map(_.toChar).mkString
    }) )
    inputDataFormat match {
      case DataFormat.JSON => spark.read.schema(tableSchema).json(dataSet)
      case DataFormat.CSV => spark.read.schema(tableSchema).csv(dataSet)
      case _ => throw new IllegalArgumentException("Input data type needs to be set as either CSV or JSON")
    }

  }

  /**
    * Writes data to the format specified.
    * @param df Dataframe to be written
    * @param dataFormat Format in which dataframe must be written
    * @param outputPath File path where data needs to be written.
    */
  def writeDataToFile(df: DataFrame, dataFormat: DataFormat.Value, outputPath : String) : Unit = {
    val dfWriter = write(df)
    dataFormat match {
      case DataFormat.PARQUET => writeToParquet(dfWriter, outputPath)
      case DataFormat.CSV => writeToCSV(dfWriter, outputPath)
      case DataFormat.JSON => writeToJSON(dfWriter, outputPath)
      case _ => throw new IllegalArgumentException("Data can be written to file and only in PARQUET | CSV | JSON formats")
    }
  }

  def write(df: DataFrame) : DataFrameWriter[Row] = {
    df
      .withColumn(YEAR_COL_NAME, lit(DateUtil.getYear()))
      .withColumn(MONTH_COL_NAME, lit(DateUtil.getMonth()))
      .withColumn(DAY_COL_NAME, lit(DateUtil.getDayOfTheMonth()))
      .write
      .mode(SaveMode.Append)
      .partitionBy(YEAR_COL_NAME, MONTH_COL_NAME, DAY_COL_NAME)
  }

  def writeToParquet(dfw: DataFrameWriter[Row], outputPath : String) : Unit = {
    dfw
      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .option("compression", "snappy")
      .parquet(outputPath)
  }

  def writeToCSV(dfw: DataFrameWriter[Row], outputPath: String) : Unit = {
    dfw
      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .csv(outputPath)
  }

  def writeToJSON(dfw: DataFrameWriter[Row], outputPath: String) : Unit = {
    dfw
      .option("mapreduce.fileoutputcommitter.algorithm.version", "2")
      .json(outputPath)
  }
}