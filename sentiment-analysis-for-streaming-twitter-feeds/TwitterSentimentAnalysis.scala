// @author: Manas Bundele
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import java.util.Properties

import org.clulab.processors.corenlp._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.kafka.clients.producer.KafkaProducer

import org.apache.kafka.clients.producer._



object TwitterSentimentAnalysis {

  def getKafkaProducer(topic: String): KafkaProducer[String, Integer] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    var producer = new KafkaProducer[String, Integer](props)
    return producer
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Twitter Streaming sentiment analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    if (args.length < 2) {
      println("Correct usage: Program_Name inputTopic outputTopic")
      System.exit(1)
    }

    val inTopic = args(0)
    val outTopic = args(1)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    import spark.implicits._

    val ssc = new StreamingContext(sc, Seconds(5))

    // change the following credentials and put your app credentials
    System.setProperty("twitter4j.oauth.consumerKey", "consumerKey")
    System.setProperty("twitter4j.oauth.consumerSecret", "consumerSecret")
    System.setProperty("twitter4j.oauth.accessToken", "accessToken-KHJYIT0Wky1K6tlo2yYFIr16wZuYvS")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "accessTokenSecret")

    val filter = Array(inTopic)

    val tweets = TwitterUtils.createStream(ssc, None, filters = filter)

    var producer = getKafkaProducer(outTopic)

    tweets.filter(_.getLang() == "en").map(status => status.getText).map(tweet => (tweet, CoreNLPSentimentAnalyzer.sentiment(tweet)))
      .foreachRDD(rdd => rdd.collect().foreach(tuple => {
        println(" Sentiment => " + tuple._2.max  + " :-: TWEET => " + tuple._1)
        val record = new ProducerRecord[String, Integer](outTopic, tuple._1, tuple._2.max)
        producer.send(record)
      }))

    ssc.start
    ssc.awaitTermination
  }
}
