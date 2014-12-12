package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs

object Transformations
{
	val fSum = (a: Int, b: Int) => a + b
	
	//Map or flatmap can take any complicated functions as long as it conforms to a given signature. 
	//In the case of flatmap, it takes a row (here Cassandra) and returns a list/seq
	def fAccDate (row: CassandraRow) = { 
		row.get[String]("accnos").split(",").toList.map(x => (x,row.get[String]("date"))).toList
	}
}
class Transformations()
{
	
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("Transformations")
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableUnusualTrans = "unusual"	//This table captures the list of acccount numbers logged for unusual transctions in a given day
	
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableUnusualTrans + " (accnos text PRIMARY KEY, date text)")
		}
	}
	
	def fillUpData()
	{
		{
			val transCollection = sc.parallelize(Seq(("abcef0,abcef4,abcef7,abcef9", "2014-12-08")))
			transCollection.saveToCassandra(keySpaceName, tableUnusualTrans, SomeColumns("accnos", "date"))
		}

		{
			val transCollection = sc.parallelize(Seq(("abcef4,abcef7,abcef8", "2014-12-09")))
			transCollection.saveToCassandra(keySpaceName, tableUnusualTrans, SomeColumns("accnos", "date"))
		}
	}
	
	def accessData()
	{
		//List of unusual activity accounts on a given day
		val unusualAct = sc.cassandraTable(keySpaceName, tableUnusualTrans)
		unusualAct.foreach(row => println( row.get[String]("accnos") + ", " + row.get[String]("date")))
		
	}

	def transformExample()
	{
		val rdd = sc.cassandraTable(keySpaceName, tableUnusualTrans)		// Read the unusual transaction records
		val accsWithDate = rdd.flatMap(Transformations.fAccDate).collect()
		println("Unusual transactions detected by account with date")
		accsWithDate.foreach(println)
		
		val accsWithDateByDate = rdd.flatMap(Transformations.fAccDate)
									.map{case (accs, date) => (date, accs)}
									.sortByKey()
									.countByKey()
		println("Number of unusual transactions detected on accounts by date")
		accsWithDateByDate.foreach(println)

		val flattenedList = rdd.flatMap(row =>row.get[String]("accnos").split(","))
								.map(acc => (acc, 1))
								.countByKey()
		println("Number of times unusual transactions detected by account")
		flattenedList.foreach(println)	
		
		val accList = rdd.flatMap(row =>row.get[String]("accnos").split(","))
							.distinct()
							.collect()
		
		println("List of Accounts where unusual transactions detected")
		accList.foreach(println)
		
		
		val accCount = rdd.flatMap(row =>row.get[String]("accnos").split(","))
						.distinct()
						.map(acc => 1)
						.reduce(Transformations.fSum)
		
		println("Number of Accounts where unusual transactions detected")				
		println (accCount) 
	}
	

	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableUnusualTrans)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
}


object TransformationsApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val transformations = new Transformations()
	  println("Creating the key space and the tables")
	  transformations.setup()
	  println("Filling in the data")
	  transformations.fillUpData()
	  
	  println("Summary of the RDDs")
	  transformations.accessData()
	  
	  println("Printing the values from transformExample operations")
	  transformations.transformExample()

	  transformations.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}