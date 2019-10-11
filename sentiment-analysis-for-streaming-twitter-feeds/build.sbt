name := "twitter-read"

version := "0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2"

scalaVersion := "2.11.12"


//val sparkVersion = "2.3.1"
//val sparkVersion = "2.2.2"
val sparkVersion = "2.4.0"


lazy val root = (project in file(".")).
  settings(
    name := "kafka",
    version := "1.0",
    scalaVersion := "2.11.12",
    mainClass in Compile := Some("kafka")
  )


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.11.12" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
  //"org.apache.kafka" %% "kafka-clients" % "2.4.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0" % "provided"
  //"org.apache.spark" %% "kafka-clients" % "0.11.0.1"
  //"org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.1",
)


libraryDependencies ++= {
  val procVer = "7.4.2"

  Seq(
    "org.clulab" %% "processors-main" % procVer,
    "org.clulab" %% "processors-corenlp" % procVer,
    "org.clulab" %% "processors-odin" % procVer,
    "org.clulab" %% "processors-modelsmain" % procVer,
    "org.clulab" %% "processors-modelscorenlp" % procVer,
  )
}

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}