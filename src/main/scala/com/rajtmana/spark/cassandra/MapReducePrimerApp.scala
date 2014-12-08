package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs

class MapReducePrimer()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("MapReducePrimer")
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableTrans = "trans"
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableTrans + " (accno text, date text, id timeuuid, amount double, PRIMARY KEY ((accno,date),id))")
		}
	}
	
	def fillUpData()
	{
		for(i <- 1 to 100){
			val accno = "abcef" + (i % 10).toString //This is to get an evenly distributed number of accounts for having different transactions
			val date = "2014-12-08"
			val timeUUID = UUIDs.timeBased()
			val amount = (1000 + (10 * i)) * scala.math.pow(-1, (i % 3))	//To get different debit and credit amounts
			val transCollection = sc.parallelize(Seq((accno, date, timeUUID, amount)))
			transCollection.saveToCassandra(keySpaceName, tableTrans, SomeColumns("accno", "date", "id", "amount"))
		}
	}
	
	def accessData()
	{
		//List the records
		val rdd = sc.cassandraTable(keySpaceName, tableTrans)
		rdd.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("id")  + ", " + row.get[String]("amount")))
		
	}

	def mapReduce(summaryFunction: (Double, Double) => Double, aggregation: String)
	{
		val rdd = sc.cassandraTable(keySpaceName, tableTrans)		// Read the transaction records
		//The following is the canonical mapping phase 
		val transactions = rdd.map(row => (row.get[String]("accno"), row.get[Double]("amount")))		//Create the account no, transaction amount tuples
		
		//The following is the canonical reducing phase
		val accSummary = transactions.reduceByKey(summaryFunction).collect()	// Summarize the amounts per account as per the function fpassed to this method
		printTuple(accSummary, aggregation)		// Print the tuple
	}

	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}

	def printTuple(summary: Array[(String, Double)], aggregation: String)
	{
		println("Acc No" + " " + aggregation)
		println("------" + " " + "--------------------------")
		for (acc <- summary)
		{
			println(acc._1 + " " + acc._2.toString)
		}
	}
	
}

object MapReducePrimerApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val mapReducePrimer = new MapReducePrimer()
	  println("Creating the key space and the tables")
	  mapReducePrimer.setup()
	  println("Filling in the data")
	  mapReducePrimer.fillUpData()
	  
	  println("Summary of the RDDs")
	  mapReducePrimer.accessData()
	  
	  println("Printing the values from map reduce operations")
	  mapReducePrimer.mapReduce((a,b) => a + b, "Total Amount")
	  mapReducePrimer.mapReduce((a,b) => (if(a < b) a else b), "Lowest Amount")
	  mapReducePrimer.mapReduce((a,b) => (if(a < b) b else a), "Highest Amount")
	  mapReducePrimer.mapReduce((a,b) => ((a + b)/2), "Average Amount")

	  mapReducePrimer.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}