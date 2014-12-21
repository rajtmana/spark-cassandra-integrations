#!/bin/bash

# [FILLUP] This is the directory name where all the Spark files are located. This will come handy when you are dealing with multiple Spark versions
SPARK_DIR=spark-1.2.0
# [FILLUP] This must point to your Spark installation bin directory
PATH_TO_SPARK_BIN_SCRIPTS=/Users/RajT/spark-source/$SPARK_DIR/bin

# [OPTIONAL] Which is your Spark master 
SPARK_MASTER=local[4]

# [FILLUP] This is your Spark Cassandra Connector version
CONNECTOR_VERSION="1.1.0"

# [FILLUP] Your Scala version
SCALA_VERSION="2.10"

# Name of the Spark Cassandra Connector jar name
CASSANDRA_CONNECTOR_ASSEMBLY_JAR="spark-cassandra-connector-assembly-$CONNECTOR_VERSION.jar"

# [FILLUP] The absolute path to the Spark Cassandra Connector jar
PATH_TO_CASSANDRA_CONNECTOR_ASSEMBLY_JAR="/Users/RajT/Documents/workspace/scala/spark/spark-cassandra-connector/spark-cassandra-connector-$CONNECTOR_VERSION/spark-cassandra-connector/target/scala-$SCALA_VERSION/$CASSANDRA_CONNECTOR_ASSEMBLY_JAR"

# Name of the application jar file. You should be OK to leave it like that
APP_JAR="spark-cassandra-integration_$SCALA_VERSION-1.0.jar"

# Absolute path to the application jar file
PATH_TO_APP_JAR="target/scala-$SCALA_VERSION/$APP_JAR"

# Spark submit command
SPARK_SUBMIT="$PATH_TO_SPARK_BIN_SCRIPTS/spark-submit"

# Pass the application name to run as the parameter to this script
APP_TO_RUN=$1

sbt package
$SPARK_SUBMIT --class $APP_TO_RUN --master $SPARK_MASTER --jars $PATH_TO_CASSANDRA_CONNECTOR_ASSEMBLY_JAR $PATH_TO_APP_JAR