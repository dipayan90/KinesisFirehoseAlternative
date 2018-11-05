# Kinesis Stream Data To Parquet


## Building the application

mvn clean install

## Deploying the application

### Locally

* Remove all provided scopes from the child pom to include required dependencies
* java -Denvironment=local -jar data-to-parquet/target/data-to-parquet-1.0.0.jar

### EMR

* Create Amazon EMR with EMR role and  EC2 instance profile role. Make sure you give appropriate permissions to these roles

* SSH using the key:

ssh -i ~/key hadoop@ip 

* Change spark-configuration: 

vi /usr/lib/spark/conf/spark-defaults.conf 

* add: 

spark.yarn.submit.waitAppCompletion   false

* Run Spark EMR Job:

[Deploying on EMR](scripts/deployment.sh)

#### Note: The jar file that you build should be uploaded to S3 first, and the emr/ec2s should have access to S3.

## Purpose

Queries s3 buckets, merge data attributes, create a view and write to another s3 bucket