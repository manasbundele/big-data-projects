// @author: Manas Bundele, Ananya Banerjee

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, StringIndexer, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object TweetProcessingAndClassification {

  def main(args: Array[String]){

    if (args.length != 2) {
      println("Usage: TweetProcessingAndClassification InputFile OutputDir")
    }

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Tweet Processing And Classification"))//.setMaster("local"))

    val spark = SparkSession
      .builder()
      .appName("Tweet Processing And Classification")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val tweets = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv(args(0)).toDF("tweet_id", "airline_sentiment", "airline_sentiment_confidence", "negativereason", "negativereason_confidence", "airline", "airline_sentiment_gold", "name", "negativereason_gold", "retweet_count", "text", "tweet_coord", "tweet_created", "tweet_location", "user_timezone")

    //tweets that have text as none
    val c=tweets.filter("text is null")

    //tweets with text != null
    val tW=tweets.filter($"text".isNotNull)

    //pipeline
    // Configure an ML pipeline, which consists of 4 stages: tokenizer, stop words remover, hashingTF, Label Conversion:

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val labelIndexer = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("label")

    //creating a logistic regression model
    import org.apache.spark.ml.classification.LogisticRegression
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, hashingTF, labelIndexer, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(tW)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(6)
      //.setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(tW)

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val finalDF = cvModel.transform(tW)
      .select("tweet_id", "text", "probability", "prediction", "label")

    // Compute raw scores on the test set
    val predictionAndLabels = finalDF.select("prediction", "label").map{x => (x(0).toString.toDouble, x(1).toString.toDouble)}.rdd

    var output = ""

    // Instantiate metrics object
    val metrics = new MulticlassMetrics(predictionAndLabels)

    // Overall Statistics
    val accuracy = metrics.accuracy
    output = output.concat("Summary Statistics:\n")
    output = output.concat("Accuracy = " + accuracy.toString + "\n")

    // Precision by label
    val labels = metrics.labels

    val precision_text = labels.map(x => "Precision (" + x.toString + ") :" + metrics.precision(x)).mkString("\n")
    output = output.concat(precision_text + "\n\n")

    // Recall by label
    val recall_text = labels.map(x => "Recall (" + x.toString + ") :" + metrics.recall(x)).mkString("\n")
    output = output.concat(recall_text + "\n\n")

    // False positive rate by label
    val fp_rate_text = labels.map(x => "False Positive Rate (" + x.toString + ") :" + metrics.falsePositiveRate(x)).mkString("\n")
    output = output.concat(fp_rate_text + "\n\n")

    // F-measure by label
    val fMeasure_text = labels.map(x => "F-Measure (" + x.toString + ") :" + metrics.fMeasure(x)).mkString("\n")
    output = output.concat(fMeasure_text + "\n\n")

    output = output.concat("Weighted precision: " +  metrics.weightedPrecision + "\n")
    output = output.concat("Weighted recall: " + metrics.weightedRecall + "\n")
    output = output.concat("Weighted F1 score: " + metrics.weightedFMeasure + "\n")
    output = output.concat("Weighted false positive rate: " + metrics.weightedFalsePositiveRate + "\n")

    val outputrdd = sc.parallelize(output.split("\n"))
    outputrdd.saveAsTextFile(args(1))
  }
}
