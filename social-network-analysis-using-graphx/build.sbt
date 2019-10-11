name := "assignment3part2"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

// allows us to include spark packages
resolvers += "apache-snapshots" at "http://repository.apache.org/snapshots/"

resolvers += "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases/"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

//libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"
)


