// @author: Manas Bundele, Ananya Banerjee
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}


object PageRank {

  val alpha=0.15

  //function finds inlinks of given airport t and return the dataframe
  def find_inlinks(data: org.apache.spark.sql.DataFrame, t: String): org.apache.spark.sql.DataFrame={
    val d1=data.filter(data("DEST")===t)//.distinct
    //val inlinks=d1.count()
    return d1

  }

  //  //function to find out-degree of an airport t in the given dataframe and return a datafrem containng all details of only airport t
  //  def find_out_data_about_airport_t(data: org.apache.spark.sql.DataFrame, t: String): org.apache.spark.sql.DataFrame={
  //    //we need to get those airports whose source = t airport
  //    val Out_Degrees=data.groupBy("ORIGIN").count().withColumnRenamed("count", "out_degree").orderBy(desc("count"))
  //    //joining original dataset with their out-degrees
  //    val f=df1.join(Out_Degrees, df1("ORIGIN")===Out_Degrees("ORIGIN")).drop(Out_Degrees.col("ORIGIN"))
  //    //obtaining the dataset with only airport t
  //    val final_df=f.filter(f("ORIGIN")===t)
  //    return final_df
  //  }

  //function to find page rank of given airport
  def find_Page_RANK(data: org.apache.spark.sql.DataFrame, t: String, spark: org.apache.spark.sql.SparkSession, N: Long): Double={
    import spark.implicits._
    //in inlinks: "t is destination"
    val inlinks_for_T= find_inlinks(data, t)
    val ratio=inlinks_for_T.withColumn("Ratio", $"PageRank"/ $"out_degree")
    val r1=ratio.agg(sum("Ratio"))
    var r2=0.0
    //println("hello"+r1.select("sum(Ratio)").show().toString())
    //println("oh"+(r1.first()(0)))
    if(r1.first()(0) != null){
      r2=r1.first()(0).toString.toDouble
    }

    val page_rank=alpha*(1/N.toDouble) + (1-alpha)*r2
    return page_rank

  }

  //function to return final dataframe with Pagerank and Airports
  def find_PR(data: org.apache.spark.sql.DataFrame, max_iter: Int, spark: org.apache.spark.sql.SparkSession, N: Long, distinct_O: Array[String]): org.apache.spark.sql.DataFrame={
    import spark.implicits._
    //For 1 iter
    var data1=data
    var data2=data
    for(i <- 1 to max_iter){
      //finding page rank of all airports
      for (x <- distinct_O){
        //find page rank of airport x
        val page_R=find_Page_RANK(data1, x, spark, N)
        //new dataframe with updated pagerank
        data2=data2.withColumn("PageRank",when($"ORIGIN"===x,page_R).otherwise(col("PageRank")))
      }
      data1=data2
    }

    return data2

  }

  def main(args: Array[String]) {

    if (args.length != 3) {
      println("Usage: PageRank InputFile NumberOfIterations OutputDir")
    }

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Tweet Processing And Classification"))//.setMaster("local"))

    val spark = SparkSession
      .builder()
      .appName("Tweet Processing And Classification")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv(args(0))

    val df1=df.drop("_c2")

    val N= df1.count()

    //finding dataframe containing out degree of all airports=="ORIGIN" col
    import org.apache.spark.sql.functions._
    val Out_Degrees=df1.groupBy("ORIGIN").count().withColumnRenamed("count", "out_degree")//.orderBy(desc("count"))

    //joining original dataset with their out-degrees
    //display(df1.join(Out_Degrees, df1("ORIGIN")===Out_Degrees("ORIGIN")).drop(Out_Degrees.col("ORIGIN")))
    val f=df1.join(Out_Degrees, df1("ORIGIN")===Out_Degrees("ORIGIN")).drop(Out_Degrees.col("ORIGIN"))//.distinct

    //initialization of column Page Rank in dataframe f
    val f2=f.withColumn("PageRank", lit(10.0))

    //list of distinct origin airports
    val distinct_O=f2.select(f2("ORIGIN")).distinct.rdd.map(r => r(0).toString).collect()

    //list of distinct destination airports
    //val distinct_D=f2.select(f2("DEST")).distinct.rdd.map(r => r(0)).collect()

    val final_df=find_PR(f2, args(1).toInt, spark, N , distinct_O)
    //final_df.show()


    val output_string = final_df.select("ORIGIN","PageRank").distinct.orderBy(desc("PageRank")).map(x => (x(0).toString + " " + x(1).toString)).collect.mkString("\n")
    val outputrdd = sc.parallelize(output_string.split("\n"))

    outputrdd.saveAsTextFile(args(2))
  }
}