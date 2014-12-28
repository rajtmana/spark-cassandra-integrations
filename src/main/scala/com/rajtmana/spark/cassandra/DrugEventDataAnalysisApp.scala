package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.SQLContext

case class Drug (primaryid: String, caseid: String, drugseq: String, rolecod: String, drugname: String)
class DrugEventDataAnalysis()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"				//Key space
	val tableDrug = "drug"				// Drug event summary table

				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableDrug + " (primaryid text, caseid text, drugseq text, rolecod text, drugname text, PRIMARY KEY ((primaryid,caseid), drugseq))")
		}
	}
	
	def fillUpData()
	{
		//The following lines of code is real power packed and deceptively simple
		//First the data from a txt file is loaded, did the transformation 
		//Converted the row into a case class object 
		//Just one call saveToCassandra does the writing to the cassandra table 
		//Right now here the data file is read from the local system where the file is located. It can even be in a Hadoop HDFS as well
		//The data file is a sample from the FDA ADVERSE EVENT REPORTING SYSTEM (FAERS)
		//The data files are publicly available from
		//http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm
		
		val drugRecs = sc.textFile("data/FAERS_DRUG14Q1_SAMPLE.csv")
						.map(line => line.split(","))
						.filter(line => line.length > 4)
						.map(line => Drug(line(0), line(1), line(2), line(3), line(4)))
		drugRecs.saveToCassandra(keySpaceName, tableDrug)		
	}
	
	def accessData()
	{
		val drugData = sc.cassandraTable(keySpaceName, tableDrug)
		drugData.foreach(println)
		println("Total number of drug records: " + drugData.count)
	}
	
	def analyzeData()
	{
		val sqlContext = new SQLContext(sc)
		import sqlContext._			//This is to get implicit access to its functions such as sql of SQLContext
		sc.cassandraTable[Drug](keySpaceName, tableDrug).registerTempTable(tableDrug)

		//Top 10 drug names for which higheset events are reported
		val strSQL1 = "SELECT drugname, COUNT(drugname) AS eventcount " + 
					 "FROM " +
					 tableDrug + " " +  
					 "GROUP BY drugname " +
					 "ORDER BY eventcount desc"
					 
		val rdd1 = sql(strSQL1).take(10)
		println("Top 10 drug names for which highest events are reported")
		rdd1.foreach(println)

		//Top 10 drug names for which lowest events are reported
		val strSQL2 = "SELECT drugname, COUNT(drugname) AS eventcount " + 
					 "FROM " +
					 tableDrug + " " +  
					 "GROUP BY drugname " +
					 "ORDER BY eventcount "
					 
		val rdd2 = sql(strSQL2).take(10)
		println("Top 10 drug names for which lowest events are reported")
		rdd2.foreach(println)
	}
	
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableDrug)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
}

object DrugEventDataAnalysisApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val drugDataAnalysis = new DrugEventDataAnalysis()
	  
	  println("Creating the key space and the tables")
	  drugDataAnalysis.setup()
	  
	  println("Filling in the data")
	  drugDataAnalysis.fillUpData()
	  
	  println("Summary of the RDDs")
	  drugDataAnalysis.accessData()

	  println("Analyze and display the data")
	  drugDataAnalysis.analyzeData()

	  drugDataAnalysis.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}