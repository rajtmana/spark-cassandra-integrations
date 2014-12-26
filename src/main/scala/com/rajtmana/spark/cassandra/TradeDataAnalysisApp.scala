package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.SQLContext

case class Trade (tickersymbol: String, tradedate: String, openamount: Double, highamount: Double, lowamount: Double, closeamount:Double, sharevolume: Long)
class TradeDataAnalysis()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"				//Key space
	val tableTrade = "trade"				// Trade summary table

				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableTrade + " (tickersymbol text, tradedate text, openamount double, highamount double, lowamount double, closeamount double, sharevolume bigint, PRIMARY KEY (tickersymbol,tradedate))")
		}
	}
	
	def fillUpData()
	{
		//The following lines of code is real power packed and deceptively simple
		//First the data from a CSV file is loaded, did the transformation 
		//Converted the row into a case class object 
		//Just one call saveToCassandra does the writing to the cassandra table 
		val tradeRecs = sc.textFile("data/NASDAQ_20101105.csv")
						.map(line => line.split(","))
						.map(line => Trade(line(0), line(1), line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble, line(6).toLong))
		tradeRecs.saveToCassandra(keySpaceName, tableTrade)		
	}
	
	def accessData()
	{
		val tradeData = sc.cassandraTable(keySpaceName, tableTrade)
		tradeData.foreach(println)
		println("Total number of trade records: " + tradeData.count)
	}
	
	def analyzeData()
	{
		val sqlContext = new SQLContext(sc)
		import sqlContext._			//This is to get implicit access to its functions such as sql of SQLContext
		sc.cassandraTable[Trade](keySpaceName, tableTrade).registerTempTable(tableTrade)

		//Top 10 change between open/close trades
		val strSQL1 = "SELECT tickersymbol, tradedate, openamount, closeamount, (openamount - closeamount) as difference " + 
					 "FROM " +
					 tableTrade + " " +  
					 "ORDER BY difference desc"
		val rdd1 = sql(strSQL1).take(10)
		println("Top 10 change between open/close trades")
		rdd1.foreach(println)
		
		//Top 10 change between high/low trades
		val strSQL2 = "SELECT tickersymbol, tradedate, highamount, lowamount, (highamount - lowamount) as difference " + 
					 "FROM " +
					 tableTrade + " " +  
					 "ORDER BY difference desc"
		val rdd2 = sql(strSQL2).take(10)
		println("Top 10 change between high/low trades")
		rdd2.foreach(println)

		//Top 10 volume trades
		val strSQL3 = "SELECT tickersymbol, tradedate, sharevolume " + 
					 "FROM " +
					 tableTrade + " " +  
					 "ORDER BY sharevolume desc"
		val rdd3 = sql(strSQL3).take(10)
		println("Top 10 volume trades")
		rdd3.foreach(println)

		//Top 10 high value stocks
		val strSQL4 = "SELECT tickersymbol, tradedate, closeamount " + 
					 "FROM " +
					 tableTrade + " " +  
					 "ORDER BY closeamount desc"
		val rdd4 = sql(strSQL4).take(10)
		println("Top 10 high value stocks")
		rdd4.foreach(println)

		//Top 10 penny stocks
		val strSQL5 = "SELECT tickersymbol, tradedate, closeamount " + 
					 "FROM " +
					 tableTrade + " " +  
					 "ORDER BY closeamount"
		val rdd5 = sql(strSQL5).take(10)
		println("Top 10 penny stocks")
		rdd5.foreach(println)

		//Aggregate numbers
		val strSQL6 = "SELECT count(*) as rowcount, max(closeamount) as maxclose, min(closeamount) as minclose,  avg(closeamount) as avgclose, " + 
					  "max(sharevolume) as maxvolume, min(sharevolume) as minvolume, avg(sharevolume) as avgvolume " +	
					  "FROM " +
					  tableTrade
					 
		val rdd6 = sql(strSQL6).collect()
		println("Aggregate Numbers")
		println("[Row Count,Max Close,Min Close,Avg Close,Max Volume,Min Volume,Avg Volume]")
		rdd6.foreach(println)
	}
	
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableTrade)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
}

object TradeDataAnalysisApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val tradeDataAnalysis = new TradeDataAnalysis()
	  
	  println("Creating the key space and the tables")
	  tradeDataAnalysis.setup()
	  
	  println("Filling in the data")
	  tradeDataAnalysis.fillUpData()
	  
	  println("Summary of the RDDs")
	  tradeDataAnalysis.accessData()

	  println("Analyze and display the data")
	  tradeDataAnalysis.analyzeData()

	  tradeDataAnalysis.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}