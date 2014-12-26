package com.rajtmana.spark.cassandra

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.SQLContext
import com.datastax.driver.core.utils.UUIDs

case class Customer(customerid: String, customername: String)
case class Order(orderno: String, date: String, customerid: String)
case class OrderLineItem(orderno: String, itemno: String, itemname: String, quantity: Int, amount: Double)

class SparkSQLJoins()
{
	// Create the Spark configuration and the spark context
	val conf = new SparkConf()
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.driver.allowMultipleContexts", "true")
				.setAppName(getClass.getSimpleName)
	
	val sc = new SparkContext(conf)
	
	//Variables for the key spaces and tables
	val keySpaceName = "test"
	val tableCustomer = "tablecustomer"					
	val tableOrder = "tableorder"					
	val tableOrderLineItem = "tablelineitem"
	
				
	def setup()
	{
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("CREATE KEYSPACE IF NOT EXISTS " + keySpaceName + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableCustomer + " (customerid text PRIMARY KEY, customername text)")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableOrder + " (orderno text, date text, customerid text, PRIMARY KEY ((orderno,date),customerid))")
		  session.execute("CREATE TABLE IF NOT EXISTS " + keySpaceName + "." + tableOrderLineItem + " (orderno text, itemno text, itemname text, quantity int, amount double, PRIMARY KEY (orderno,itemno))")
		}
	}
	
	def fillUpData()
	{
		for(i <- 0 to 10){
			val customerid = "cust" + i.toString
			val customername = "Customer " + customerid
			val customerCollection = sc.parallelize(Seq((customerid, customername)))
			customerCollection.saveToCassandra(keySpaceName, tableCustomer, SomeColumns("customerid", "customername"))

		}
		for(i <- 1 to 10){
			val orderno = "order" + i.toString //Get an evenly distributed number of accounts for having different transactions
			val customerid = "cust" + (i % 5).toString
			val date = "2014-12-1" + (i % 10).toString
			val orderCollection = sc.parallelize(Seq((orderno, date, customerid)))
			orderCollection.saveToCassandra(keySpaceName, tableOrder, SomeColumns("orderno", "date", "customerid"))
			for(j <- 1 to 3){
				val itemno = "item-" + i.toString + "-"+ j.toString
				val itemname = "Name of " + itemno
				val quantity = ((i + 5) % 8	) + 5	
				val amount = (1000 + (10.0 * i)) 	
				val lineCollection = sc.parallelize(Seq((orderno, itemno, itemname, quantity,amount)))
				lineCollection.saveToCassandra(keySpaceName, tableOrderLineItem, SomeColumns("orderno", "itemno", "itemname", "quantity","amount"))
			}
		}
	}
	
	def accessData()
	{
		//List the records
		sc.cassandraTable(keySpaceName, tableCustomer).foreach(println)
		sc.cassandraTable(keySpaceName, tableOrder).foreach(println)
		sc.cassandraTable(keySpaceName, tableOrderLineItem).foreach(println)
	}


	def joinSqlQuery()
	{
		val sqlContext = new SQLContext(sc)
		import sqlContext._			//This is to get implicit access to its functions such as sql of SQLContext
		sc.cassandraTable[Customer](keySpaceName, tableCustomer).registerTempTable(tableCustomer)
		sc.cassandraTable[Order](keySpaceName, tableOrder).registerTempTable(tableOrder)
		sc.cassandraTable[OrderLineItem](keySpaceName, tableOrderLineItem).registerTempTable(tableOrderLineItem)
		
		
		//List all the orders and order line items
		val strSQL1 = "SELECT o.date, o.customerid, o.orderno, l.itemno, l.itemname, l.quantity, l.amount, l.quantity * l.amount " + 
					 "FROM " +
					 tableOrder + " AS o JOIN " +  
					 tableOrderLineItem + " AS l " +
					 "WHERE o.orderno = l.orderno " +
					 "ORDER BY o.date, o.orderno, l.itemno"
		val rdd1 = sql(strSQL1)
		rdd1.collect().foreach(println)
		
		//List all the customer details and if there are orders, list their details as well
		val strSQL2 =  "SELECT c.customername, o.customerid, o.orderno, o.date " + 
						"FROM " +
						tableCustomer + " AS c " + 
						"LEFT OUTER JOIN " +
						tableOrder + " AS o " + 
						"ON c.customerid=o.customerid " +
						"ORDER BY c.customername"
		val rdd2 = sql(strSQL2)
		rdd2.collect().foreach(println)

		//List only the customer details only for them having orders
		val strSQL3 =  "SELECT c.customername, o.customerid, o.orderno, o.date " + 
						"FROM " +
						tableOrder + " AS o " + 
						"LEFT OUTER JOIN " +
						tableCustomer + " AS c " + 
						"ON c.customerid=o.customerid " +
						"ORDER BY c.customername"
		val rdd3 = sql(strSQL3)
		rdd3.collect().foreach(println)

		//List all the customer details and if there are orders, list their details as well
		val strSQL4 =  "SELECT c.customername, o.customerid, o.orderno, o.date " + 
						"FROM " +
						tableOrder + " AS o " + 
						"RIGHT OUTER JOIN " +
						tableCustomer + " AS c " + 
						"ON c.customerid=o.customerid " +
						"ORDER BY c.customername"
		val rdd4 = sql(strSQL4)
		rdd4.collect().foreach(println)
	}
	
	
	
	def cleanupCassandraObjects()
	{
		//Cleanup the tables and key spaces created
		CassandraConnector(conf).withSessionDo { session =>
		  session.execute("DROP TABLE " + keySpaceName + "." + tableCustomer)
		  session.execute("DROP TABLE " + keySpaceName + "." + tableOrder)
		  session.execute("DROP TABLE " + keySpaceName + "." + tableOrderLineItem)
		  session.execute("DROP KEYSPACE " + keySpaceName)
		}
	}	
}

object SparkSQLJoinsApp {
  def main(args: Array[String]) 
  {
	  println("Start running the program") 
	  val sparkSQLJoins = new SparkSQLJoins()
	  
	  println("Creating the key space and the tables")
	  sparkSQLJoins.setup()
	  
	  println("Filling in the data")
	  sparkSQLJoins.fillUpData()
	  
	  println("Summary of the RDDs")
	  sparkSQLJoins.accessData()
	  
	  println("SQL Queries")
	  sparkSQLJoins.joinSqlQuery()

	  sparkSQLJoins.cleanupCassandraObjects()
	  println("Successfully completed running the program")
  }
}