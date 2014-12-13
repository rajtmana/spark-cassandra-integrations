package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.driver.core.utils.UUIDs

case class Account(key: String, amount: Double)
class SparkSQLPrimer()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableTrans = "trans"				//Transaction table
				
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
		val rdd1 = sc.cassandraTable(keySpaceName, tableTrans)
		rdd1.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("id")  + ", " + row.get[String]("amount")))
	}

	def sqlQuery()
	{
		val cc = new CassandraSQLContext(sc)
		val rdd = cc.cassandraSql("SELECT * FROM " + keySpaceName + "." + tableTrans)
		rdd.collect().foreach(println)
	}
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		sc.stop()
	}	
}

object SparkSQLPrimerApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val sparkSQLPrimer = new SparkSQLPrimer()
	  println("Creating the key space and the tables")
	  sparkSQLPrimer.setup()
	  println("Filling in the data")
	  sparkSQLPrimer.fillUpData()
	  
	  println("Summary of the RDDs")
	  sparkSQLPrimer.accessData()
	  
	  println("SQL Queries")
	  sparkSQLPrimer.sqlQuery()

	  sparkSQLPrimer.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}