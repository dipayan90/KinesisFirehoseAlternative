#!/usr/bin/env bash

spark-submit \
     --master yarn \
     --deploy-mode client \
     --executor-cores 1 \
     --driver-java-options "-Denvironment=prod -Dappname=kinesisToParquet" \
     --executor-memory 8GB \
     --num-executors 50 \
     --conf spark.dynamicAllocation.enabled=true \
     --conf spark.yarn.executor.memoryOverhead=2GB \
     --conf spark.shuffle.memoryFraction=0.5 \
     --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -Denvironment=prod -Dappname=kinesisToParquet" \
     --conf spark.repartition.trigger=false \
     --conf spark.kinesis.stream.name="query-logs" \
     --conf spark.kinesis.assume.role.arn="arn:aws:iam::004323233598:role/nonprod-sts-kinesis-s3-prod" \
     --conf spark.batch.interval.time.in.minutes="5" \
     --conf spark.repartition.multiplier=1 \
     --conf spark.output.path=s3a://cdp-ml-dev/ql \
     --conf spark.output.delete.previous.trigger=true \
     --class com.kajjoy.ds.App \
    ./data-to-parquet-1.0.0.jar