package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.rdd._

class DataAccessAuditUseCase()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"				//Key space
	val tableTrans = "trans"				// Transaction table
	val tableHighVal = "highval"			// Audit table containing high value transactions
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableTrans + " (accno text, date text, id timeuuid, amount double, PRIMARY KEY ((accno,date),id))")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableHighVal + " (accno text, date text, id timeuuid, amount double, PRIMARY KEY ((accno,date),id))")
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
		val fullTable = sc.cassandraTable(keySpaceName, tableTrans)

		//List the records by reading all the values from the table
		fullTable.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("date")  + ", " +row.get[String]("id")  + ", " + row.get[String]("amount")))
				
	}

	def auditData()
	{
		val fullTable = sc.cassandraTable(keySpaceName, tableTrans)
		//Select all the columns and only a few records in the table where the transaction amount is greater than a given number
		val rdd = 	fullTable
					.filter(row => row.get[Double]("amount") > 1900.0)
		// Prepare an RDD after applying many transformations
		// If you want to move the resultant data to another Cassandra table, it is a very simple operation
		// You just call the saveToCassandra method with the target table details 
		rdd.saveToCassandra(keySpaceName, tableHighVal)
		val auditTable = sc.cassandraTable(keySpaceName, tableHighVal)
		auditTable.foreach(row => println( row.get[String]("accno") + ", " + row.get[String]("date")  + ", " +row.get[String]("id")  + ", " + row.get[String]("amount")))
		
	}
	
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP TABLE " + keySpaceName + "." + tableHighVal)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
}

object DataAccessAuditUseCaseApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val dataAccessAuditUseCase = new DataAccessAuditUseCase()
	  
	  println("Creating the key space and the tables")
	  dataAccessAuditUseCase.setup()
	  
	  println("Filling in the data")
	  dataAccessAuditUseCase.fillUpData()
	  
	  println("Summary of the RDDs")
	  dataAccessAuditUseCase.accessData()

	  println("Create the audit table and display the data")
	  dataAccessAuditUseCase.auditData()

	  dataAccessAuditUseCase.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}