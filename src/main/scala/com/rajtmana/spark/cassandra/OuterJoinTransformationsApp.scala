package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs

class OuterJoinTransformations()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("OuterJoinTransformationsApp")
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	//Table Realation: account to trans => One to Many
	//All the relationships are modelled with the key accno
	val keySpaceName = "test"
	val tableAcc = "account"				//Accounts master table
	val tableTrans = "trans"				//Transaction table
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableAcc + " (accno text PRIMARY KEY, fname text, lname text)")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableTrans + " (accno text, date text, id timeuuid, amount double, PRIMARY KEY ((accno,date),id))")
		}
	}
	
	def fillUpData()
	{
		//Create more accounts in the master table
		for (i <- 0 to 9){
			val accno = "abcef" + i.toString
			val fname = "FirstName" + i.toString
			val lname = "LastName" + i.toString
			val accCollection = sc.parallelize(Seq((accno, fname, lname)))
			accCollection.saveToCassandra(keySpaceName, tableAcc, SomeColumns("accno", "fname", "lname"))
		}
		
		//Create transactions only for a few of the accounts to demonstrate the difference when you do the outer joins
		for(i <- 1 to 50){
			val accno = "abcef" + (i % 5).toString //Get an evenly distributed number of accounts for having different transactions
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
		
	}

	def joinTables()
	{
		//Spark supports caching and if an RDD is being used again and again, it makes sense to use caching mechanism
		//Hence all the main data sets have been cached
		//Here the number of records are very less so caching makes sense. Use your discretion while doing so when deploying production code
		val rddAcc = sc.cassandraTable(keySpaceName, tableAcc).cache				// Read the account master table
		val rddTrans = sc.cassandraTable(keySpaceName, tableTrans).cache			// Read the transaction records

		//One to Many relation. For one account, there are zero or more transactions
		//Right Outer Join
		//List ONLY the accounts with transaction records
		//Print the account master details along with the transactions
		//Scala Pattern matching is a great way to split the multiple levels of the key value pair nesting (see the map below)
		rddAcc.map(row => (row.get[String]("accno"), row.get[String]("fname")))
				.rightOuterJoin(rddTrans.map(row => (row.get[String]("accno"), (row.get[String]("date"),row.get[String]("amount")))))
				.sortByKey()
				.map{
					case (accno,(Some(fname),(date,amount))) => (accno, fname, date, amount)
				}
				.foreach(println)

		
		//One to Many relation. For one account, there are zero or more transactions
		//There are accounts without any transaction. Print those as well with "No Transaction" and 0 amount
		//Left Outer Join
		//Print the account master details along with the transactions
		//Scala Pattern matching is a great way to split the multiple levels of the key value pair nesting (see the map below)
		rddAcc.map(row => (row.get[String]("accno"), row.get[String]("fname")))
				.leftOuterJoin(rddTrans.map(row => (row.get[String]("accno"), (row.get[String]("date"),row.get[String]("amount")))))
				.sortByKey()
				.map{
					//This is the case where there are transactions for the accounts
					case (accno, (fname, Some((date, amount)))) => (accno, fname, date, amount)
					//This is the case where there are NO transactions for the accounts
					case (accno, (fname, None)) => (accno, fname, "No Transaction", 0.0)
				}
				.foreach(println)
	
	
				
	}
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableAcc)
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}	
}

object OuterJoinTransformationsApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val outerJoins = new OuterJoinTransformations()
	  println("Creating the key space and the tables")
	  outerJoins.setup()
	  println("Filling in the data")
	  outerJoins.fillUpData()
	  
	  println("Summary of the RDDs")
	  outerJoins.accessData()
	  
	  println("Table Join Results")
	  outerJoins.joinTables()

	  outerJoins.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}