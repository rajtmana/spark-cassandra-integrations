package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs

class TransformationsPrimer()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("TransformationsPrimer")
	
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

	def transformExample1(summaryFunction: (Double, Double) => Double, aggregation: String)
	{
		val rdd = sc.cassandraTable(keySpaceName, tableTrans)		// Read the transaction records
		
		//First filter the deposits, then create the account number and transaction amount tuple
		val deposits = rdd
						.filter(_.get[Double]("amount") > 0)
						.map(row => (row.get[String]("accno"), row.get[Double]("amount")))		//Create the account no, transaction amount tuples
		//First filter the withdrawals and then create the account number and transaction amount tuple				
		val withdrawals = rdd
						.filter(_.get[Double]("amount") < 0)
						.map(row => (row.get[String]("accno"), row.get[Double]("amount")))		//Create the account no, transaction amount tuples
		
		//The following combines the deposits and withdrawals to form the complete set
		//The whole idea of doing this is to demonstrate the use of union
		val allTransactions = deposits.union(withdrawals)
		
		//All the below operations are summarized by Key and apply the aggregation function and get the values
		printAccAmount(deposits.reduceByKey(summaryFunction).collect(), aggregation + "(deposits)")		// Print the tuple
		printAccAmount(withdrawals.reduceByKey(summaryFunction).collect(), aggregation + "(withdrawals)")		// Print the tuple
		//Apart from the generic operations mentioned above, this sorts the records by the account number as well
		printAccAmount(allTransactions.reduceByKey(summaryFunction).sortByKey().collect(), aggregation)		// Print the tuple 		
		println("Original Data Set Count: " + rdd.count().toString + ", Combined Data Set Count: " + allTransactions.count().toString + ", Deposits Data Set Count: " + deposits.count().toString + ", Withdrawals Data Set Count: " + withdrawals.count().toString)
	}
	
	def transformExample2()
	{
		val rdd = sc.cassandraTable(keySpaceName, tableTrans)		// Read the transaction records
		//First filter the deposits, then create the account number and transaction amount tuple
		val deposits = rdd
						.filter(_.get[Double]("amount") > 0)
						.map(row => (row.get[String]("accno"), row.get[Double]("amount")))		//Create the account no, transaction amount tuples
						
		//Get the list of minimum deposits by account				
		val minDeposits = deposits.reduceByKey(Functions.fMin)
		
		//Get the list of maximum deposits by account
		val maxDeposits = deposits.reduceByKey(Functions.fMax)
		
		//Group by account, get the minimum and maximum deposits
		val minAndMaxDeposits = minDeposits.union(maxDeposits).groupByKey().sortByKey().collect()
		//Print it
		printAccAmounts(minAndMaxDeposits,"Min/Max Deposits")
		
		//Get the number of deposits by account
		val depositCounts = deposits.countByKey()
		println("Number of deposits by account")
		depositCounts.foreach(println)

		//Highest and lowest deposits across all the accounts
		val depositAmounts = rdd
						.filter(_.get[Double]("amount") > 0)
						.map(row => (row.get[Double]("amount"), row.get[String]("accno")))		//Create the transaction amount, account no tuples

		//In the below example, instead of take(1), you can also use first(). In that case it will return (Double, String) instead of Array[(Double, String)] in this use case
		val highestDeposit = depositAmounts.sortByKey(false).take(1)
		val lowestDeposit = depositAmounts.sortByKey(true).take(1)
		printAmountAcc(highestDeposit, "Highest Deposit")
		printAmountAcc(lowestDeposit, "Lowest Deposit")
	}

	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrans)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}

	def printAccAmount(summary: Array[(String, Double)], aggregation: String)
	{
		println("Acc No" + " " + aggregation)
		println("------" + " " + "--------------------------")
		for (acc <- summary)
		{
			println(acc._1 + " " + acc._2.toString)
		}
	}
	
	def printAmountAcc(summary: Array[(Double, String)], aggregation: String)
	{
		println("Acc No" + " " + aggregation)
		println("------" + " " + "--------------------------")
		for (acc <- summary)
		{
			println(acc._2 + " " + acc._1.toString)
		}
	}


	def printAccAmounts(summary: Array[(String, Iterable[Double])], aggregation: String)
	{
		println("Acc No" + " " + aggregation)
		println("------" + " " + "--------------------------")
		for (acc <- summary)
		{
			//println(acc._1 + " " + acc._2[0].toString + "," + acc._2[1].toString)
			println(acc._1 + " " + acc._2.toList)
		}
	}

	
}

//Function definitions that can be used in multiple places
object Functions {
	val fSum = (a: Double, b:Double) => a + b
	val fMin = (a: Double, b:Double) => (if(a < b) a else b)
	val fMax = (a: Double, b:Double) => (if(a > b) a else b)
	val fAvg = (a: Double, b:Double) => (a + b)/2.0
}

object TransformationsPrimerApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val transformationsPrimer = new TransformationsPrimer()
	  println("Creating the key space and the tables")
	  transformationsPrimer.setup()
	  println("Filling in the data")
	  transformationsPrimer.fillUpData()
	  
	  println("Summary of the RDDs")
	  transformationsPrimer.accessData()
	  
	  println("Printing the values from transformExample1 operations")
	  transformationsPrimer.transformExample1(Functions.fSum, "Total Amount")
	  transformationsPrimer.transformExample1(Functions.fMin, "Lowest Amount")
	  transformationsPrimer.transformExample1(Functions.fMax , "Highest Amount")
	  transformationsPrimer.transformExample1(Functions.fAvg, "Average Amount")

	  println("Printing the values from transformExample2 operations")
	  transformationsPrimer.transformExample2()
	  

	  transformationsPrimer.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}