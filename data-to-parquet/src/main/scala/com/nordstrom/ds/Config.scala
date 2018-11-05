package com.nordstrom.ds

import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder

/**
  * Created by bjmc on 3/22/18.
  */
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

  def getS3Client(deploymentEnvironment: String) = {
    if(deploymentEnvironment.equals("local")){
      AmazonS3ClientBuilder
        .standard()
        .withCredentials(new ProfileCredentialsProvider("nordstrom-federated"))
        .build()
    } else {
      AmazonS3ClientBuilder
        .standard()
        .withCredentials( InstanceProfileCredentialsProvider.getInstance())
        .build()
    }
  }

}