package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.utils.UUIDs

class SparkCassandraDataTypes()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName("SparkCassandraDataTypes")
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableWeather = "weather"
				
	def setup()
	{
		//Create a table with complex and various available data types supported by Cassandra
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableWeather + " (weatherStation text, date text, locationId bigint, details blob, active boolean, sourceDevice inet, stationMasters LIST<text>, emails SET<text>, coordinates MAP<Int, Int>, timeUUID timeuuid, PRIMARY KEY ((weatherstation,date),timeuuid))")
		}
	}
	
	def fillUpData()
	{
		//Create variables of the supported data types, have the initial value and use them to insert into the Cassandra table
		val weatherStation  = "This one" 
		val date = "2014-12-07"
		val locationId: Long = 0
		val details = Array[Byte](1, 2, 3)
		val active = true
		val sourceDevice: java.net.InetAddress = java.net.InetAddress.getLocalHost
		val stationMasters = List("me", "you")
		val emails = Set("someone@email.com", "s@email.com")
		val coordinates = Map(1 -> 1, 2 -> 2)
		val timeUUID: java.util.UUID = UUIDs.timeBased()
		
		//Insert data into the Cassandra table
		val fieldCollection = sc.parallelize(Seq((weatherStation, date, locationId, details, active, sourceDevice, stationMasters, emails, coordinates, timeUUID)))
		fieldCollection.saveToCassandra(keySpaceName, tableWeather, SomeColumns("weatherstation", "date", "locationid", "details", "active", "sourcedevice", "stationmasters", "emails", "coordinates", "timeuuid"))

	}
	
	def accessData()
	{
		//Summary of the RDD
		val rdd = sc.cassandraTable(keySpaceName, tableWeather)
		val firstRow = rdd.first
		println("Accessing string value by passing data type (weatherStation): " + firstRow.get[String]("weatherstation"))
		println("Accessing string value by passing data type (date): " + firstRow.get[String]("date"))
		println("Accessing long value by passing data type (locationId): " + firstRow.get[Long]("locationid"))
		println("Accessing byte array value by passing data type (details): " + firstRow.get[Array[Byte]]("details"))
		println("Accessing boolean value by passing data type (active): " + firstRow.get[Boolean]("active"))
		println("Accessing java.net.InetAddress value by passing data type (sourceDevice): " + firstRow.get[java.net.InetAddress]("sourcedevice"))
		println("Accessing List value by passing data type (stationMasters): " + firstRow.get[List[String]]("stationmasters"))
		println("Accessing Set value by passing data type (emails): " + firstRow.get[Set[String]]("emails"))
		println("Accessing Map value by passing data type (coordinates): " + firstRow.get[Map[String, String]]("coordinates"))
		println("Accessing UUID value by passing data type (timeUUID): " + firstRow.get[java.util.UUID]("timeuuid"))
	}
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableWeather)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
		
	}
	
}

object DataTypesApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val sparkCassandraDataTypes = new SparkCassandraDataTypes()
	  println("Creating the key space and the tables")
	  sparkCassandraDataTypes.setup()
	  println("Filling in the data")
	  sparkCassandraDataTypes.fillUpData()
	  println("Summary of the RDDs")
	  sparkCassandraDataTypes.accessData()
	  println("Cleaning up the tables and keyspaces")
	  sparkCassandraDataTypes.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}