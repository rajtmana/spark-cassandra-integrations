package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs

class TableJoins()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("TableJoinsApp")
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableAcc = "account"
	val tableTrans = "trans"
	val tableIP = "ips"
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableAcc + " (accno text PRIMARY KEY, fname text, lname text)")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableTrans + " (accno text, date text, id timeuuid, amount double, PRIMARY KEY ((accno,date),id))")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableIP + " (accno text, ipaddress text, date text, PRIMARY KEY ((accno,ipaddress),date))")
		}
	}
	
	def fillUpData()
	{
		for (i <- 0 to 9){
			val accno = "abcef" + i.toString
			val fname = "FirstName" + i.toString
			val lname = "LastName" + i.toString
			val accCollection = sc.parallelize(Seq((accno, fname, lname)))
			accCollection.saveToCassandra(keySpaceName, tableAcc, SomeColumns("accno", "fname", "lname"))
			
			val date = "2014-12-08"
			val ipAddress = "192.168.1.10"
			val ipCollection = sc.parallelize(Seq((accno, ipAddress, date)))
			ipCollection.saveToCassandra(keySpaceName, tableIP, SomeColumns("accno", "ipaddress", "date"))			
		}
		for(i <- 1 to 100){
			val accno = "abcef" + (i % 10).toString //Get an evenly distributed number of accounts for having different transactions
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
		val rdd0 = sc.cassandraTable(keySpaceName, tableAcc)
		rdd0.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("fname")  + ", " + row.get[String]("lname")))
		
		val rdd1 = sc.cassandraTable(keySpaceName, tableTrans)
		rdd1.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("id")  + ", " + row.get[String]("amount")))
		
		val rdd2 = sc.cassandraTable(keySpaceName, tableIP)
		rdd2.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("ipaddress")  + ", " + row.get[String]("date")))
		
	}

	def joinTables()
	{
		val rddAcc = sc.cassandraTable(keySpaceName, tableAcc).cache				// Read the account master table
		val rddTrans = sc.cassandraTable(keySpaceName, tableTrans).cache			// Read the transaction records
		val rddIPAddress = sc.cassandraTable(keySpaceName, tableIP).cache					// Read the IP table
		
		//One to Many relation. For one account, there are multiple transactions
		//Print the account master details along with the transactions
		//Scala Pattern matching is a great way to split the multiple levels of the key value pair nesting (see the map below)
		rddAcc.map(row => (row.get[String]("accno"), row.get[String]("fname")))
				.join(rddTrans.map(row => (row.get[String]("accno"), (row.get[String]("date"),row.get[String]("amount")))))
				.map{case (accno, (fname, (date, amount))) => (accno, fname, date, amount)}
				.foreach(println)
				
				

		//One to One relation. For one account, there is one IP address table
		//Note the use of the distinct method call in the joining clause. This is to eliminate multiple records on the key
		//Print the account master details along with the ip address details
		//Scala Pattern matching is a great way to split the multiple levels of the key value pair nesting (see the map below)
		rddAcc.map(row => (row.get[String]("accno"), row.get[String]("fname")))
				.join(rddIPAddress.map(row => (row.get[String]("accno"), (row.get[String]("date"),row.get[String]("ipaddress")))).distinct())
				.map{case (accno, (fname, (date, ipaddress))) => (accno, fname, date, ipaddress)}
				.foreach(println)
				
	
		//Many to One relation. Multiple transactions are joined with the account master
		//Print the account master details along with the transactions
		//Scala Pattern matching is a great way to split the multiple levels of the key value pair nesting (see the map below)
		rddTrans.map(row => (row.get[String]("accno"), row.get[String]("amount")))
				.join(rddAcc.map(row => (row.get[String]("accno"), row.get[String]("fname"))))
				.map{case (accno, (amount, (fname))) => (accno, fname, amount)}
				.foreach(println)

	}
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableAcc)
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP TABLE " + keySpaceName + "." + tableIP)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}	
}

object TableJoinsApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val tableJoins = new TableJoins()
	  println("Creating the key space and the tables")
	  tableJoins.setup()
	  println("Filling in the data")
	  tableJoins.fillUpData()
	  
	  println("Summary of the RDDs")
	  tableJoins.accessData()
	  
	  println("Table Join Results")
	  tableJoins.joinTables()

	  tableJoins.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}