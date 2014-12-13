# Apache Spark/Apache Cassandra Applications in Scala

## Introduction
This is a repository containing "getting started" kind of scala code for using Apache Spark with Apache Cassandra. All the code base given in this repository are run on the following versions of the softawres:

1. Apache Spark - Built from the latest code from the trunk
2. Apache Cassandra - Built from the latest code from the trunk
3. Datastax Spark Cassandra Connector - Built from the latest code from the trunk
4. Scala version 2.10.4

Note that all these code samples are developed and tested on a single node Cassandra and single node Spark running on a Macbook Pro with OSX 10.9.5

**DISCLAIMER**

1. The code may not be properly formatted (all the text editors are not made equal) or may not follow all the best practices of proper scala coding. The whole idea is to expose and to educate the Spark-Cassandra integration features.
2. Error handling in the code is completely avoided as sprinkling of a lot of that will take away the attention that is to be given to the functionality that is being demonstrated.
3. Absolutely no warranty on the code.

# Follow me.... @rajtmana

## Pre-Requisites
1. Make sure that Cassandra is running in your machine
2. Make sure that Scala with sbt is working in your machine
3. Make sure that Spark is available and ready to run in your machine
4. The run.sh script file contains paths to many of the libraries such as the Spark bin directory, paths to the spark-cassandra-connector jar, the application jar etc. So before running the first application, make sure that all the paths are relevant to your setup. The rule of thumb on where to make change is that wherever you see a [FILLUP], your changes are required there. You are OK to leave the others. There are some lines marked with [OPTIONAL], you are OK to leave it with the defaults

## Applications List
1. com.rajtmana.spark.cassandra.StarterApp - This is a starter application that is creating some RDDs, reading/writing from/to Cassandra tables
2. com.rajtmana.spark.cassandra.DataTypesApp - This is an application that is used to create a Cassandra table with various complex data types, save data into those tables and access them. The goal is to demonstrate the use of various complex data types and how they have to be dealt with while saving it into Cassandra tables
3. com.rajtmana.spark.cassandra.MapReducePrimerApp - This is an application that implements a primer version of a typical map/reduce kind of use case. The example used here is a bank account transactions of multiple accounts. This summarizes various parameters by account such as total of transaction amounts, lowest of the transaction amounts, highest of the transaction amounts, average of the transaction amounts per account. The transaction table is created in Cassandra, sample records have been filled in, read the values from the Cassandra tables and did all the activities.
4. com.rajtmana.spark.cassandra.TransformationsPrimerApp - This is an application that is using a bank account transaction use case and demonstrate the use of many important Spark transformations and actions. The transaction table is created in Cassandra, sample records have been filled in, read the values from the Cassandra tables and did all the activities.
5. com.rajtmana.spark.cassandra.TransformationsApp - This is an application covering more Spark transformations using the bank's black listed accounts spotted for unusual transactions. The account list is created in Cassandra, sample records have been filled in, read the values from the Cassandra table and did all the activities.
6. com.rajtmana.spark.cassandra.TransformationsMoreApp - Cassandra CQL does not support table joins. But Spark provides powerful features in their transformations library to do that. This is sample application where a bank transactions coming from one Cassandra table and the IP addresses of these transactions coming from another table. Using join/cogroup transformation, the tables are joined and the relevant information is taken out
7. com.rajtmana.spark.cassandra.DataAccessPrimerApp - This is an application covering various use cases of accessing data from Cassandra tables. This includes full table selection, selection with filtering, receiving the selected records into Scala objects using case classes and used Spark Accummulators to collect values while processing the RDD records.
8. com.rajtmana.spark.cassandra.TableJoinsApp - This is an application demonstrating the Many-to-One, One-to-Many, One-to-One Cassandra table joins. Cassandra tables are created for Account, Transactions, and IP Address. All these relationships are modelled mainly using Spark's join in conjunction with map and some other transformations.
9. com.rajtmana.spark.cassandra.SparkSQLPrimerApp - Cassandra CQL does not support WHERE clauses on non-indexed/primary key columns. Spark SQL supports. This application demonstrates that.


## How to Run
1. Make sure that the code is compiling in your setup. For that run ```./compile.sh ```
2. Run your code by giving the application name to the run script as the first command line option. For that run ```./run.sh com.rajtmana.spark.cassandra.StarterApp``` The list of applications is listed in the code sample list above and the whole path is to be used as given in the above list
3. Each application is separated out in separate files are supposed to be run independently and serves its own unique purpose. 
4. Each application will setup the required Cassandra keyspace/tables requred to run that application and at the end of the run, all of them will be destroyed so that the application run is not polluting your setup in anyway

# Follow me.... @rajtmana