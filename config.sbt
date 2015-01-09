name := "Spark Cassandra Integration"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "1.2.0"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"