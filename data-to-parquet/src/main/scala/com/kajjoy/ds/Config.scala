package com.kajjoy.ds

object Config {

  def getDeploymentEnvironment: String = {
    System.getProperty("environment")
  }

  def getAppName : String = {
    System.getProperty("appname")
  }

  def getOutPutS3Path : String = {
    System.getProperty("outputpath")
  }

}