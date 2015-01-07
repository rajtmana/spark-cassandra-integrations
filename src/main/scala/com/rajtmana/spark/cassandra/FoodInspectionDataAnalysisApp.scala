package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.SQLContext

case class FoodInspec (license: String, zip: String, inspectionid: String, state: String, inspectiontype: String, facilitytype: String, name:String, city: String, akaname: String, risk: String, inspectiondate: String, results: String, address: String, longitude: String, latitude: String)
class FoodInspectionDataAnalysis()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"				//Key space
	val tableFoodInspec = "foodinspec"		// Food Inspection table

				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableFoodInspec + " (license text, zip text, inspectionid text, state text, inspectiontype text, facilitytype text, name text, city text, akaname text, risk text, inspectiondate text, results text, address text, longitude text, latitude text, PRIMARY KEY (license,inspectionid))")
		}
	}
	
	def fillUpData()
	{
		//The following lines of code is real power packed and deceptively simple
		//First the data from a CSV file is loaded, did the transformation 
		//Converted the row into a case class object 
		//Just one call saveToCassandra does the writing to the cassandra table 
		val foodRecs = sc.textFile("data/FoodInspectionData.csv")
						.map(line => line.split(","))
						.map(line => FoodInspec(line(0), line(1), line(2), line(3), line(4), line(5), line(6),line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14)))
		foodRecs.saveToCassandra(keySpaceName, tableFoodInspec)		
	}
	
	def accessData()
	{
		val inspecData = sc.cassandraTable(keySpaceName, tableFoodInspec)
		println("Total number of food inspection records: " + inspecData.count)
	}
	

	def analyzeData()
	{
		val sqlContext = new SQLContext(sc)
		import sqlContext._			//This is to get implicit access to its functions such as sql of SQLContext
		sc.cassandraTable[FoodInspec](keySpaceName, tableFoodInspec).registerTempTable(tableFoodInspec)

		//Top 10 organizations where inspection happened
		val strSQL1 = "SELECT name, count(name) as occurcount " + 
					 "FROM " +
					 tableFoodInspec + " " +  
					 "GROUP BY name " +
					 "ORDER BY occurcount desc"
		val rdd1 = sql(strSQL1).take(10)
		println("Top 10 organizations where inspection happened")
		rdd1.foreach(println)
		
		//Top 10 failed inspections
		val strSQL2 = "SELECT name, count(name) as occurcount " + 
					 "FROM " +
					 tableFoodInspec + " " +  
					 "WHERE results = 'Fail' " + 
					 "GROUP BY name " +
					 "ORDER BY occurcount desc "
		val rdd2 = sql(strSQL2).take(10)
		println("Top 10 failed inspections")
		rdd2.foreach(println)

		//Top 10 facility types with failed inspections
		val strSQL3 = "SELECT facilitytype, count(facilitytype) as occurcount " + 
					 "FROM " +
					 tableFoodInspec + " " +  
					 "WHERE results = 'Fail' " + 
					 "GROUP BY facilitytype " +
					 "ORDER BY occurcount desc "
		val rdd3 = sql(strSQL3).take(10)
		println("Top 10 facility types with failed inspections")
		rdd3.foreach(println)
		
		//Top 10 risks on failed inspections with occurrence count
		val strSQL4 = "SELECT risk, count(risk) as occurcount " + 
					 "FROM " +
					 tableFoodInspec + " " +  
					 "WHERE results = 'Fail' " + 
					 "GROUP BY risk " +
					 "ORDER BY occurcount desc "
		val rdd4 = sql(strSQL4).take(10)
		println("Top 10 risks on failed inspections with occurrence count")
		rdd4.foreach(println)

		//Top 10 conditional pass inspections
		val strSQL5 = "SELECT name, count(name) as occurcount " + 
					 "FROM " +
					 tableFoodInspec + " " +  
					 "WHERE results = 'Pass w/ Conditions' " + 
					 "GROUP BY name " +
					 "ORDER BY occurcount desc "
		val rdd5 = sql(strSQL5).take(10)
		println("Top 10 conditional pass inspections")
		rdd5.foreach(println)
	
		//Top 10 inspection failed restaurants
		val strSQL6 = "SELECT name, count(name) as occurcount " + 
					 "FROM " +
					 tableFoodInspec + " " +  
					 "WHERE results = 'Fail' and facilitytype = 'Restaurant'" + 
					 "GROUP BY name " +
					 "ORDER BY occurcount desc "
					 
		val rdd6 = sql(strSQL6).take(10)
		println("Top 10 inspection failed restaurants")
		rdd6.foreach(println)

		//Restaurants gone Out of business
		val strSQL7 = "SELECT name, address " + 
					 "FROM " +
					 tableFoodInspec + " " +  
					 "WHERE results = 'Out of Business' and facilitytype = 'Restaurant'" + 
					 "ORDER BY name"
					 
		val rdd7 = sql(strSQL7).collect()
		println("Restaurants gone out of business")
		rdd7.foreach(println)


	}
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableFoodInspec)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
}

object FoodInspectionDataAnalysisApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val foodInspecDataAnalysis = new FoodInspectionDataAnalysis()
	  
	  println("Creating the key space and the tables")
	  foodInspecDataAnalysis.setup()
	  
	  println("Filling in the data")
	  foodInspecDataAnalysis.fillUpData()
	  
	  println("Summary of the RDDs")
	  foodInspecDataAnalysis.accessData()

	  println("Analyze and display the data")
	  foodInspecDataAnalysis.analyzeData()

	  foodInspecDataAnalysis.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}