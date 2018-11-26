# Kinesis Stream Data To Parquet

## What is it ?

This application allows you to just configure an incoming kinesis stream,
and convert the incoming data into the format you like. Not just that
it enables you to buffer data up up to a configurable timeframe and write it out.

Features:

* Configure kinesis stream.
* Support for STS Assume role for kinesis stream ( Incase EMR cluster is on a different
 AWS account as the stream )
* Supports  custom transformation using input schema definitions.
* Supports following input data types:
  - json
  - csv
* Customizable buffer size based off buffer time interval.
* Supports following output types:
    - parquet
    - json
    - csv

## Configuration

| Config Name                                        | Possible Values                                                                                                                                                | Data Type              |
|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------|
| spark.kinesis.stream.name                          | Name of the kinesis stream                                                                                                                                     | String                 |
| spark.kinesis.assume.role.arn                      | Kinesis Stream Assume role arn. Eg: "Resource": arn:aws:kinesis:*:111122223333:stream/my-stream                                                                | String                 |
| spark.aws.region                                   | Your AWS Region                                                                                                                                                | String                 |
| spark.input.record.sample                          | A JSON that describes the schema of the input records in the kinesis stream. Example: "{'authorization' : '', 'host' : '['abc', 'def']'}"                      | String                 |
| spark.batch.interval.time.in.minutes               | Provide the buffer interval in minutes.                                                                                                                        | String                 |
| spark.output.path                                  | Path where the buffered data needs to be written. Eg: s3a://bucket-name/bucket/prefix/filename                                                                 | String                 |
| spark.input.record.format                          | Input data format for the stream can be either JSON or CSV                                                                                                     | String                 |
| spark.output.record.format                         | Format in which data will be written out to the file. Can be either PARQUET or JSON or CSV                                                                     | String                 |


## Building the application

mvn clean install

## Deploying the application

### Locally

* Remove all provided scopes from the child pom to include required dependencies
* java -Denvironment=local -jar data-to-parquet/target/data-to-parquet-1.0.0.jar

### Production

* If running on EMR all the AWS depdencies should already be available
on the master node. So, you could just use as is. For how to deploy this application
on EMR look at [deployment.sh](scripts/deployment.sh)