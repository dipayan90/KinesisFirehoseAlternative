package com.kajjoy.ds

import org.apache.spark.sql.SparkSession

/**
  * Created by bjmc on 3/27/18.
  */
trait TestSparkContextProvider {

  def getTestSparkContext(): SparkSession = {
    System.setProperty("appname", "TestApp")
    App.initializeSpark("local")
  }

}
