package com.rajtmana.spark.cassandra

import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.rdd._

class EventDataAnalysis()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"				//Key space
	val tableEvent = "events"				// Events Transaction table
	
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableEvent + " (eventtype text, eventdate text, eventtime timestamp, eventid timeuuid, impact int, urgency int, status int, message text, PRIMARY KEY ((eventtype,eventdate),eventtime))")
		}
	}
	
	def fillUpData()
	{
		for(i <- 1 to 111)
		{
			val eventtype = if (i < 30) "I" else (if (i < 60) "W" else "E")
			val eventdate = "2014-12-1" + (i % 7).toString
			val eventtime = Calendar.getInstance.getTime
			val eventid = UUIDs.timeBased()
			val impact = ((i % 3) + 1)
			val urgency = (((i+1) % 3) + 1)
			val status = (i % 2)
			val message = "This is the " + i.toString + " event"
			val eventCollection = sc.parallelize(Seq((eventtype, eventdate, eventtime, eventid, impact, urgency, status, message )))
			eventCollection.saveToCassandra(keySpaceName, tableEvent, SomeColumns("eventtype", "eventdate", "eventtime", "eventid", "impact", "urgency", "status", "message"))
		}
	}
	
	def accessData()
	{
		val fullTable = sc.cassandraTable(keySpaceName, tableEvent)
		
		//List the records by reading all the values from the table
		fullTable.foreach(println)
				
	}
	
	def eventTypeAnalysis()
	{
		val fullTable = sc.cassandraTable(keySpaceName, tableEvent)
		val eventsByType = fullTable
							.map(row => (row.get[String]("eventtype"), 1))		//Extract event types (map stage)
							.reduceByKey((a,b) => a + b)						//Count the event types (reduce stage)
							.sortByKey()
							.map(row => ("Event Type " + row._1 + ", Event Count: " + row._2.toString))
							.foreach(println)									//Print them

		val eventsByDate = fullTable
							.map(row => (row.get[String]("eventdate"), 1))		//Extract event dates (map stage)
							.reduceByKey((a,b) => a + b)						//Count the event types (reduce stage)
							.sortByKey()
							.map(row => ("Event Date " + row._1 + ", Event Count: " + row._2.toString))
							.foreach(println)									//Print them

		val eventsByImpact = fullTable
							.map(row => (row.get[String]("impact"), 1))			//Extract event impact (map stage)
							.reduceByKey((a,b) => a + b)						//Count the event types (reduce stage)
							.sortByKey()
							.map(row => ("Event Impact " + row._1 + ", Event Count: " + row._2.toString))
							.foreach(println)									//Print them

		val eventsByUrgency = fullTable
							.map(row => (row.get[String]("urgency"), 1))			//Extract event urgency (map stage)
							.reduceByKey((a,b) => a + b)						//Count the event types (reduce stage)
							.sortByKey()
							.map(row => ("Event Urgency " + row._1 + ", Event Count: " + row._2.toString))
							.foreach(println)									//Print them

		val eventsByStatus = fullTable
							.map(row => (row.get[String]("status"), 1))			//Extract event status (map stage)
							.reduceByKey((a,b) => a + b)						//Count the event types (reduce stage)
							.sortByKey()
							.map(row => ("Event Status " + row._1 + ", Event Count: " + row._2.toString))
							.foreach(println)									//Print them
				
	}
		
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableEvent)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
}

object EventDataAnalysisApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val eventData = new EventDataAnalysis()
	  
	  println("Creating the key space and the tables")
	  eventData.setup()
	  
	  println("Filling in the data")
	  eventData.fillUpData()
	
	  println("Summary of the RDDs")
	  eventData.accessData()
	  eventData.eventTypeAnalysis()

	  eventData.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}