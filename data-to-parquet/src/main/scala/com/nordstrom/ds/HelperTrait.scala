package com.nordstrom.ds

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait HelperTrait {

  def convertByteArrayToDataFrame(rdd: RDD[Array[Byte]], tableSchema : StructType)(implicit spark: SparkSession): DataFrame  = {
    import spark.implicits._
    val dataSet : Dataset[String] = spark.createDataset(rdd.map(w => {
      w.map(_.toChar).mkString
    }) )
    spark.read.schema(tableSchema).json(dataSet)
  }
}