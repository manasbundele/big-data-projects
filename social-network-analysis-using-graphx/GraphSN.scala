// @author: Manas Bundele, Ananya Banerjee
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

object GraphSN {

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: GraphSN InputFile OutputDir")
    }

    val sc = new SparkContext(new SparkConf().setAppName("Analysis of Social Networks using GraphX"))//.setMaster("local"))

    val spark = SparkSession
      .builder()
      .appName("Analysis of Social Networks using GraphX")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    import spark.implicits._

    val data = spark.read.option("delimiter"," ").csv(args(0)).toDF("src","dst")

    val src = data.select("src").distinct

    val dest = data.select("dst").distinct

    val vertices = src.union(dest).withColumnRenamed("src", "id")

    val edges = data.withColumn("relationship",lit("follow"))

    // Create a GraphFrame
    val graph = GraphFrame(vertices, edges)
    graph.cache()

    var output_str = ""

    val outdegree = graph.outDegrees.sort(desc("outDegree"))
    output_str = output_str.concat("Top 5 Outdegrees:\nFormat: Node Count\n")
    output_str = output_str.concat(outdegree.map(x => (x(0).toString + " " + x(1).toString)).take(5).mkString("\n"))

    val indegree = graph.inDegrees.sort(desc("inDegree"))
    output_str = output_str.concat("\n\nTop 5 Indegrees:\nFormat: Node Count\n")
    output_str = output_str.concat(indegree.map(x => (x(0).toString + " " + x(1).toString)).take(5).mkString("\n"))

    val ranks = graph.pageRank.resetProbability(0.15).maxIter(10).run()
    output_str = output_str.concat("\n\nTop 5 nodes with highest pagerank:\nFormat: Node Pagerank\n")
    output_str = output_str.concat(ranks.vertices.orderBy(desc("pagerank")).select("id", "pagerank").distinct.map(x => (x(0).toString + " " + x(1).toString)).take(5).mkString("\n"))

    // checkpointing
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

    val connected_components = graph.connectedComponents.run()
    output_str = output_str.concat("\n\nTop 5 components with largest number of nodes:\nFormat: Component Count\n")
    output_str = output_str.concat(connected_components.select("id", "component").groupBy("component").count().orderBy(desc("count")).map(x => (x(0).toString + " " + x(1).toString)).take(5).mkString("\n"))

    val triangle_count = graph.triangleCount.run()
    output_str = output_str.concat("\n\nTop 5 nodes with largest triangle count:\nFormat: Node Count\n")
    output_str = output_str.concat(triangle_count.select("id", "count").orderBy(desc("count")).distinct.map(x => (x(0).toString + " " + x(1).toString)).take(5).mkString("\n"))

    val outputrdd = sc.parallelize(output_str.split("\n\n"))

    outputrdd.saveAsTextFile(args(1))
  }
}
