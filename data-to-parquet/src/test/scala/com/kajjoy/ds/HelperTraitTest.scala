package com.kajjoy.ds

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class HelperTraitTest extends TestSparkContextProvider {

  @Test
  def testConvertByteArrayToDataFrame(): Unit = {
    implicit val spark: SparkSession = getTestSparkContext()
    import spark.implicits._
    val json1 = jsonCreator("auth1", 1, 100, 1000, "a")
    val json2 = jsonCreator("auth2", 2, 200, 2000, "b")

    val data: RDD[Array[Byte]] = spark.sparkContext.parallelize(Seq(json1, json2))

    object TestApp extends HelperTrait
    val tableSchema : StructType = spark.read.json(Seq("""{"authorization" : ""}""".stripMargin).toDS()).schema
    val df: DataFrame = TestApp.convertByteArrayToDataFrame(data, tableSchema, DataFormat.JSON)
    df.show(false)
  }


  def jsonCreator(auth: String, documentVersion : Long, skuId: Long, styleId : Long, userId: String): Array[Byte] = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    val json =
      ("authorization" -> auth) ~
        ("breadcrumb" -> "String") ~
        ("environmentName" -> "String") ~
        ("host" -> List("test1", "test2")) ~
        ("documentversion" -> documentVersion) ~
        ("skuid" -> skuId) ~
        ("styleid" -> styleId) ~
        ("requestUriQuery" -> "String") ~
        ("requestUriStem" -> "String") ~
        ("timeStamp" -> "String") ~
        ("traceContext" -> "String") ~
        ("userId" -> userId)
    compact(render(json)).toCharArray.map(_.toByte)
  }
}
