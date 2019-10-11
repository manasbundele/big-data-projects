// @author: Manas Bundele, Ananya Banerjee
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.linalg.Vector
import collection.mutable
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors


object TopicModeling {

  def main(args: Array[String]) {

    if (args.length != 2) {
      println("Usage: TopicModeling InputFile OutputDir")
    }

    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Topic Modeling"))//.setMaster("local"))

    // "/Users/manasbundele/Documents/big data/assignment/assignment2/src/main/scala/hyde.txt"
    val corpus: RDD[String] = sc.wholeTextFiles(args(0)).map(_._2)
    corpus.take(10)

    // Split each document into a sequence of terms (words)
    val stopWordSet = StopWordsRemover.loadDefaultStopWords("english").toSet

    val tokenized: RDD[Seq[String]] =
      corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(token => !stopWordSet.contains(token)).filter(_.forall(java.lang.Character.isLetter)))

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
    tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

    //   vocabArray: Chosen vocab (removing common terms)
    val numStopwords = 20
    val vocabArray: Array[String] =
      termCounts.takeRight(termCounts.size - numStopwords).map(_._1)

    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }

    // Set LDA parameters
    val numTopics = 5
    val lda = new LDA().setK(numTopics).setMaxIterations(10)

    val ldaModel = lda.run(documents)

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
//    topicIndices.foreach { case (terms, termWeights) =>
//      println("TOPIC:")
//      terms.zip(termWeights).foreach { case (term, weight) =>
//        println(s"${vocabArray(term.toInt)}\t$weight")
//      }
//      println()
//    }

    // writing to text file in proper format
    var output = ""
    topicIndices.foreach { case (terms, termWeights) =>
      var t=""
      terms.zip(termWeights).foreach { case (term, weight) =>
        t = t.concat(s"${vocabArray(term.toInt)}\t$weight\n")
      }
      output = output.concat("TOPIC:\n").concat(t).concat("\n")
    }

    val outputrdd = sc.parallelize(output.split(" "))

    outputrdd.saveAsTextFile(args(1))

  }
}

