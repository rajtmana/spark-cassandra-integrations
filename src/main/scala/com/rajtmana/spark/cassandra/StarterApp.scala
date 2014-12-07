package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector


object StarterApp {
  def main(args: Array[String]) 
  {
	println("Start running the program")  
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableWords = "words"
	val tableUsers = "users"
	
	println("Creating the key space and the tables")
	CassandraConnector(conf).withSessionDo { session =>
	  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
	  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableWords + " (word text PRIMARY KEY, count int)")
	  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableUsers + " (username text PRIMARY KEY, emails SET<text>)")
	}
	
	println("Filling in the data")
	val wordCollection = sc.parallelize(Seq(("the", 5), ("an", 6)))
	wordCollection.saveToCassandra(keySpaceName, tableWords, SomeColumns("word", "count"))
	val emails = Set("someone@email.com", "s@email.com")
	val emailCollection = sc.parallelize(Seq(("someone", emails)))
	emailCollection.saveToCassandra(keySpaceName, tableUsers, SomeColumns("username", "emails"))
	
	
	
	//Summary of the RDD
	println("Summary of the RDDs")
	val rdd = sc.cassandraTable(keySpaceName, tableWords)
	val firstRow = rdd.first
	println("Number of records: " + rdd.count)
	println("First record: " + firstRow)
	println("Sum of all the word counts: " + rdd.map(_.getInt("count")).sum)
	println("Column Names: " + firstRow.columnNames)
	println("Column Size: " + firstRow.size)
	println("Accessing integer value: " + firstRow.getInt("count"))
	println("Accessing long value: " + firstRow.getLong("count"))
	println("Aggregate value of all the records: " + rdd.map(_.getInt("count")).sum)
	
	//Generic queries by passing the return type directly.
	println("Accessing integer value by passing data type: " + firstRow.get[Int]("count"))
	println("Accessing long value by passing data type: " + firstRow.get[Long]("count"))
	println("Accessing bigint value by passing data type: " + firstRow.get[BigInt]("count"))
	println("Accessing java.math.BigInteger value by passing data type: " + firstRow.get[java.math.BigInteger]("count"))
	
	//How to avoid null pointer exception, use Option type of the Scala
	println("Accessing IntOption value: " + firstRow.getIntOption("count"))
	println("Accessing IntOption value by passing data type: " + firstRow.get[Option[Int]]("count"))


	//Query the collection set
	val userRow = sc.cassandraTable(keySpaceName, tableUsers).first
	println("Printing the values from a collection set")
	println(userRow.getList[String]("emails"))            // Vector(someone@email.com, s@email.com)
	println(userRow.get[List[String]]("emails"))          // List(someone@email.com, s@email.com)    
	println(userRow.get[Seq[String]]("emails"))           // List(someone@email.com, s@email.com)   :Seq[String]
	println(userRow.get[IndexedSeq[String]]("emails"))    // Vector(someone@email.com, s@email.com) :IndexedSeq[String]
	println(userRow.get[Set[String]]("emails"))     	
	
	//SQL like way of processing records
	println("SQL Like way of processing the records")
	sc.cassandraTable(keySpaceName, tableWords).select("word").toArray.foreach(println)
	sc.cassandraTable(keySpaceName, tableWords).select("count").toArray.foreach(println)
	sc.cassandraTable(keySpaceName, tableUsers).select("username").toArray.foreach(println)
	sc.cassandraTable(keySpaceName, tableUsers).select("emails").toArray.foreach(println)
	
	
	//Cleanup the tables and key spaces created
	println("Cleaning up the tables and keyspaces")
	CassandraConnector(conf).withSessionDo { session =>
	  session.execute("DROP TABLE " + keySpaceName + "." + tableWords)
	  session.execute("DROP TABLE "  + keySpaceName + "." + tableUsers)
	  session.execute("DROP KEYSPACE " + keySpaceName)
	}
	println("Successfully completed running the program")
	
  }
}