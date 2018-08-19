#!/usr/bin/env bash

# If Kafka is used
KAFKA_JARS=`ls /apps/kafka/*.jar`

# If Stanford CoreNLP is used
CORENLP_JARS=`ls /apps/corenlp/*.jar |xargs | tr ' ' ','`

# set this to your app name
APP_JAR="/apps/TextFeatures.jar"

# ALl JARs
JARS="${APP_JAR},${KAFKA_JARS},${CORENLP_JARS}"

echo "adding jars ${JARS}"

# Starting Spark Shell
/spark/bin/spark-shell \
    --jars ${JARS} \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 \
    --conf spark.driver.extraJavaOptions="-Dscala.color" \
    --executor-memory 8G \
    --driver-memory 8G
