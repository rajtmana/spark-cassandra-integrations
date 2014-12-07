#!/bin/bash

# Pass the application name to run as the parameter to this script
PATH_TO_SPARK_BIN_SCRIPTS=/Users/RajT/spark-source/spark-trunk/bin
SPARK_MASTER=local[4]
APP_TO_RUN=$1
CONNECTOR_VERSION="1.1.1-SNAPSHOT"
SCALA_VERSION="scala-2.10"
CASSANDRA_CONNECTOR_JAR="spark-cassandra-connector-assembly-$CONNECTOR_VERSION.jar"
PATH_TO_CASSANDRA_CONNECTOR_JAR="/Users/RajT/Documents/workspace/scala/spark/spark-cassandra-connector/spark-cassandra-connector/target/$SCALA_VERSION/$CASSANDRA_CONNECTOR_JAR"
APP_JAR="spark-cassandra-integration_2.10-1.0.jar"
PATH_TO_APP_JAR="target/$SCALA_VERSION/$APP_JAR"
SPARK_SUBMIT="$PATH_TO_SPARK_BIN_SCRIPTS/spark-submit"

sbt package
$SPARK_SUBMIT --class $APP_TO_RUN --master $SPARK_MASTER --jars $PATH_TO_CASSANDRA_CONNECTOR_JAR $PATH_TO_APP_JAR