package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.rdd._

class DataAccessPrimer()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("DataAccessPrimer")
	
	val sc = new SparkContext(conf)
	case class Account(accno: String, date: String, id: java.util.UUID, amount: Double) extends Serializable
	case class ShortAccount(accno: String, amount: Double) extends Serializable
	
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
			val date = "2014-12-1" + (i % 10).toString
			val timeUUID = UUIDs.timeBased()
			val amount = (1000 + (10 * i)) * scala.math.pow(-1, (i % 3))	//To get different debit and credit amounts
			
			val transCollection = sc.parallelize(Seq((accno, date, timeUUID, amount)))
			transCollection.saveToCassandra(keySpaceName, tableTrans, SomeColumns("accno", "date", "id", "amount"))
		}
	}
	
	def accessData()
	{
		//List the records by reading all the values from the table
		val rdd1 = sc.cassandraTable(keySpaceName, tableTrans)
		rdd1.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("date")  + ", " +row.get[String]("id")  + ", " + row.get[String]("amount")))
		
		//Select only a few columns in the table
		val rdd2 = sc.cassandraTable(keySpaceName, tableTrans)
					.select("accno", "amount")
		rdd2.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("amount")))
		
		//Select only a few columns and only a few records in the table
		val rdd3 = sc.cassandraTable(keySpaceName, tableTrans)
					.select("accno", "amount")
					.where("accno = ? and date = ?", "abcef0", "2014-12-10")
		rdd3.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("amount")))
		
		//Mapping rows to (case) objects, select including a where clause
		val rdd4 = sc.cassandraTable[Account](keySpaceName, tableTrans)
					.select("accno", "date", "id", "amount")
					.where("accno = ? and date = ?", "abcef1", "2014-12-11")
		rdd4.foreach(Account => println(Account.accno + ", " + Account.amount))

		//Mapping rows to (case) objects, select including a where clause. 
		//The object contains only a selected fields as compared to the selection list
		//This case also demonstrates the use of a selection statement containing more columns and the receiving object has less columns
		val rdd5 = sc.cassandraTable[ShortAccount](keySpaceName, tableTrans)
					.select("accno", "date", "id", "amount")
					.where("accno = ? and date = ?", "abcef2", "2014-12-12")
		rdd5.foreach(ShortAccount => println(ShortAccount.accno + ", " + ShortAccount.amount))
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

object DataAccessPrimerApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val dataAccessPrimer = new DataAccessPrimer()
	  println("Creating the key space and the tables")
	  dataAccessPrimer.setup()
	  println("Filling in the data")
	  dataAccessPrimer.fillUpData()
	  
	  println("Summary of the RDDs")
	  dataAccessPrimer.accessData()

	  dataAccessPrimer.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}