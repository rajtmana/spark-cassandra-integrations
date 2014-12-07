# Apache Spark - Apache Cassandra Integration
This is a repository containing "getting started" kind of scala code for using Apache Spark with Apache Cassandra. All the code base given in this repository are run on the following versions of the softawres
1. Apache Spark - Built from the latest code from the trunk
2. Apache Cassandra - Built from the latest code from the trunk
3. Datastax Spark Cassandra Connector - Built from the latest code from the trunk

Note that all these code samples are developed and tested on a single node Cassandra and single node Spark running on a Macbook Pro with OSX 10.9.5

**DISCLAIMER**

1. The code may not be properly formatted or may not follow all the best practices of proper scala coding. The whole idea is to expose and educate the Spark-Cassandra integration features.
2. Absolutely no warranty on the code

## Pre-Requisites
1. Make sure that Cassandra is running in your machine
2. Make sure that you have Scala 2.10.4 or above
3. Make sure that Spark is available and ready to run in your machine

## Code Sample List
1. com.rajtmana.spark.cassandra.StarterApp - This is a starter application that is creating some RDDs, reading/writing from/to Cassandra tables 

## How to Run
1. Make sure that the code is compiling in your setup. For that run ```./compile.sh ```
2. Run your code by giving the application name to the run script as the first command line option. For that run ```./run.sh com.rajtmana.spark.cassandra.StarterApp```
3. Each application is separated out in separate files are supposed to be run independently and serves its own unique purpose. 
4. Each application will setup the required Cassandra keyspace/tables requred to run that application and at the end of the run, all of them will be destroyed so that the application run is not polluting your setup in anyway

# Follow me.... @rajtmana