package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs

class TransformationsMore()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("TransformationsMore")
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableTrans = "trans"
	val tableIP = "ips"
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableTrans + " (accno text, date text, id timeuuid, amount double, PRIMARY KEY ((accno,date),id))")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableIP + " (accno text, ipaddress text, date text, PRIMARY KEY ((accno,ipaddress),date))")
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
		for(i <- 1 to 10){
			val accno = "abcef" + (i % 10).toString //This is to get an evenly distributed number of accounts for having different
			val date = "2014-12-08"
			val ipAddress = "192.168.1.10"
			val ipCollection = sc.parallelize(Seq((accno, ipAddress, date)))
			ipCollection.saveToCassandra(keySpaceName, tableIP, SomeColumns("accno", "ipaddress", "date"))
		}
	}
	
	def accessData()
	{
		//List the records
		val rdd1 = sc.cassandraTable(keySpaceName, tableTrans)
		rdd1.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("id")  + ", " + row.get[String]("amount")))
		
		val rdd2 = sc.cassandraTable(keySpaceName, tableIP)
		rdd2.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("ipaddress")  + ", " + row.get[String]("date")))
		
	}

	def transformExample()
	{
		val rddTrans = sc.cassandraTable(keySpaceName, tableTrans).cache		// Read the transaction records
		val rddIP = sc.cassandraTable(keySpaceName, tableIP).cache					// Read the IP table
		val transList = rddTrans.map(row => (row.get[String]("accno"), row.get[String]("amount")))		//Get all the Transactions
		val ipList = rddIP.map(row => (row.get[String]("accno"), row.get[String]("ipaddress")))			//Get all the IP addresses

		//Print a list of account numbers, transaction amounts, ip addresses
		transList.join(ipList).foreach(row => println("Acc No: " + row._1 + ", Trans Amount and IP Address: " + row._2))		

		//Print a summary record of account numbers, all the transaction amounts, the ip address by account
		transList.cogroup(ipList).foreach(row => println("Acc No: " + row._1 + ", Transaction Amounts: " + row._2._1.toList + ", IP Addresses: " + row._2._2.toList))	
	}
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP TABLE " + keySpaceName + "." + tableIP)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}	
}

object TransformationsMoreApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val transformationsMore = new TransformationsMore()
	  println("Creating the key space and the tables")
	  transformationsMore.setup()
	  println("Filling in the data")
	  transformationsMore.fillUpData()
	  
	  println("Summary of the RDDs")
	  transformationsMore.accessData()
	  
	  println("Printing the values from transformExample operations")
	  transformationsMore.transformExample()

	  transformationsMore.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}