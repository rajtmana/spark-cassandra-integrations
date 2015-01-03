package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.rdd._

class BroadcastVar()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)

	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableTrans = "trans"
	
	//A broadcast variable is a read-only variable that is made available from the driver program running the SparkContext, 
	//to the nodes that will execute the computation. 
	//This is very useful in applications that need to make the same data available to the worker nodes in an efficient manner, 
	//such as machine learning algorithms.	
	
	//Here is a list containing list of privileged accounts that needs to be made available to all the worker nodes
	val privAccList = sc.broadcast(List("abcef3", "abcef7"))
				
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
			val date = "2014-12-1" + (i % 10).toString
			val timeUUID = UUIDs.timeBased()
			val amount = (1000 + (10 * i)) * scala.math.pow(-1, (i % 3))	//To get different debit and credit amounts
			
			val transCollection = sc.parallelize(Seq((accno, date, timeUUID, amount)))
			transCollection.saveToCassandra(keySpaceName, tableTrans, SomeColumns("accno", "date", "id", "amount"))
		}
	}
	
	def accessData()
	{
		val fullTable = sc.cassandraTable(keySpaceName, tableTrans).cache

		//List the records by reading all the values from the transaction table
		fullTable.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("date")  + ", " +row.get[String]("id")  + ", " + row.get[String]("amount")))
	}

	def privAccData()
	{
		val localPrivAccList = privAccList.value // Create a local copy of privileged accounts from the boradcast list
		val privTrans = sc.cassandraTable(keySpaceName, tableTrans).filter(row => localPrivAccList.contains(row.get[String]("accno")))

		//List the selected records 
		privTrans.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("date")  + ", " +row.get[String]("id")  + ", " + row.get[String]("amount")))
	}

	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
}

object BroadcastVarApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val broadcastVar = new BroadcastVar()
	  println("Creating the key space and the tables")
	  broadcastVar.setup()
	  println("Filling in the data")
	  broadcastVar.fillUpData()
	  
	  println("Summary of the RDDs")
	  broadcastVar.accessData()

	  println("Transactions of privileged accounts")
	  broadcastVar.privAccData()

	  broadcastVar.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}