import sys
import pandas as pd
from pyspark.conf import SparkConf
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import WordNetLemmatizer
from textblob import TextBlob
from nltk import pos_tag
import re
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.feature import IDF, StringIndexer, StopWordsRemover, CountVectorizer, Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import udf
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DataType, StringType
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from sklearn.utils import resample
from pyspark.sql.functions import col
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LinearSVC
from pyspark.ml.feature import HashingTF, Tokenizer,IDF
from pyspark.ml.feature import NGram
from pyspark.ml.clustering import KMeans
from pyspark.ml.clustering import LDA
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand


### Util Functions ###

def get_wordnet_pos(word):
    tag = nltk.pos_tag(str(word))[0][1][0].upper()

    tag_dic = {"J": wn.ADJ, "N": wn.NOUN, "V": wn.VERB, "R": wn.ADV}

    return tag_dic.get(tag, wn.NOUN)


def resample(s, ratio):
    df_not_fake = s[s.label == 1]
    df_fake = s[s.label == 0]

    not_fake_total = df_not_fake.count()
    fake_total = df_fake.count()

    total_values = not_fake_total + fake_total
    not_fake_frac = not_fake_total/total_values
    fake_frac = fake_total/total_values

    if not_fake_frac>0.6:
        fraction_val=float(fake_total*ratio)/float(not_fake_total)
        sampled = df_not_fake.sample(False,fraction_val)
        return sampled.union(df_fake)
    elif fake_frac>0.6:
        fraction_val=float(not_fake_total*ratio)/float(fake_total)
        sampled = df_fake.sample(False,fraction_val)
        return sampled.union(df_not_fake)
    else:
        return s


### Pre-processing ###

def pre_processing(cf):
    # Converting label -1 -> 0
    cf = cf.withColumn("label",
                       f.when(cf["label"]==-1,0).
                       otherwise(cf["label"].cast(IntegerType())))

    # removing punctuations
    cf_pl = cf.rdd.map(lambda x: (re.sub(r'[^\w\s]','',x.review).lower(),x.label)).toDF(["review","label"])

    # class imbalance solved here
    cf_pl = resample(cf_pl, 2)

    # Tokenize the reviews
    tokenizer = Tokenizer(inputCol="review", outputCol="tokenized")
    t = tokenizer.transform(cf_pl)

    # removing stop words
    stopwords_remover = StopWordsRemover(inputCol="tokenized", outputCol="filtered")
    s = stopwords_remover.transform(t)

    # removed empty strings in tokenized arrays
    s = s.rdd.map(lambda x: (x.review, x.label, x.tokenized, [y.strip() for y in x.filtered if y.strip()])).toDF(["review","label","tokenized","filtered"])

    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    try:
        s = s.rdd.map(lambda x: ([(lemmatizer.lemmatize(y, get_wordnet_pos(y))) for y in x.filtered])).toDF(["review","label","tokenized","filtered", "lemmatized"])
    except:
        # incase it fails, we use the filtered column
        s = s.withColumn('lemmatized', s.filtered)

    # temporary variable swap
    class_balancedDf = s

    # randomly shuffling class_balancedDf
    class_balancedDf = class_balancedDf.orderBy(rand())

    return class_balancedDf


### Feature Engineering ###

def feature_engineering(class_balancedDf):
    # N-Gram
    ngram = NGram(n=2, inputCol="lemmatized", outputCol="ngrams")
    ngramDataFrame = ngram.transform(class_balancedDf)

    # Hashing TF
    hashingTF = HashingTF(inputCol="ngrams", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(ngramDataFrame)

    # IDF
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    # K-Means
    kmeans = KMeans().setK(6).setSeed(1)
    kmodel = kmeans.fit(rescaledData).transform(rescaledData)

    #LDA
    lda = LDA(k=10, maxIter=10)
    ldamodel = lda.fit(kmodel).transform(kmodel)

    # changing label column to int
    data = ldamodel.withColumn("label", ldamodel.label.cast("Integer")).drop("prediction")

    return data


### Different models ###

def naive_bayes(training_data, test_data, output_str):
    nb = NaiveBayes(modelType="multinomial")
    paramGrid = ParamGridBuilder().addGrid(nb.smoothing, [0.01, 0.1, 1.0, 10, 100]).build()
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    cv = CrossValidator(estimator=nb, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
    cvModel = cv.fit(training_data)
    cvPredictions = cvModel.transform(test_data)

    # Evaluate bestModel found from Cross Validation
    accuracy = evaluator.evaluate(cvPredictions)
    output_str = output_str + "Naive Bayes accuracy is: " + str(accuracy) + "\n"

    predictionandLabels = cvPredictions.withColumn('label1', cvPredictions["label"].cast("double")).select("prediction","label1").rdd
    metrics = BinaryClassificationMetrics(predictionandLabels)

    auroc = metrics.areaUnderROC
    aupr = metrics.areaUnderPR
    output_str = output_str + "NB Area under ROC Curve: " + str(auroc) + "\n"
    output_str = output_str + "NB Area under PR Curve: " + str(aupr) + "\n"

    return output_str


def random_forest(training_data, test_data, output_str):
    rf = RandomForestClassifier(labelCol="label", featuresCol="features")
    paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [20, 50, 80]) \
        .addGrid(rf.maxDepth, [3, 5, 10, 15]) \
        .build()
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    crossval = CrossValidator(
        estimator=rf,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=5)

    rfmodel = crossval.fit(training_data)

    rfPredictions = rfmodel.transform(test_data)

    # Evaluate bestModel found from Cross Validation
    accuracy = evaluator.evaluate(rfPredictions)
    output_str = output_str + "Random Forest accuracy is: " + str(accuracy) + "\n"

    predictionandLabels = rfPredictions.withColumn('label1', rfPredictions["label"].cast("double")).select("prediction","label1").rdd
    metrics = BinaryClassificationMetrics(predictionandLabels)

    auroc = metrics.areaUnderROC
    aupr = metrics.areaUnderPR
    output_str = output_str + "RF Area under ROC Curve: " + str(auroc) + "\n"
    output_str = output_str + "RF Area under PR Curve: " + str(aupr) + "\n"

    return output_str


def gradient_boosting(training_data, test_data, output_str):
    # Train a GBT model.
    gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)

    # model = gbt.fit(trainingData)

    paramGrid = (ParamGridBuilder()
                 .addGrid(gbt.maxDepth, [2, 3, 4, 6])
                 .addGrid(gbt.maxIter, [20, 50])
                 .addGrid(gbt.stepSize, [0.1, 0.01, 1])
                 .build())
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    cv = CrossValidator(estimator=gbt, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

    cvModel = cv.fit(training_data)

    predictions = cvModel.transform(test_data)

    accuracy = evaluator.evaluate(predictions)
    output_str = output_str + "Gradient Boosted Trees accuracy is: " + str(accuracy) + "\n"

    predictionandLabels = predictions.withColumn('label1', predictions["label"].cast("double")).select("prediction","label1").rdd
    metrics = BinaryClassificationMetrics(predictionandLabels)

    auroc = metrics.areaUnderROC
    aupr = metrics.areaUnderPR
    output_str = output_str + "GBT Area under ROC Curve: " + str(auroc) + "\n"
    output_str = output_str + "GBT Area under PR Curve: " + str(aupr) + "\n"

    return output_str


def svm(training_data, test_data, output_str):

    lsvc = LinearSVC()

    # Fit the model

    paramGrid = ParamGridBuilder().addGrid(lsvc.regParam, [0.1, 0.001, 0.01, 10]).addGrid(lsvc.maxIter, [10, 100, 1000]).build()

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

    crossval = CrossValidator(estimator=lsvc,
                              estimatorParamMaps=paramGrid,
                              evaluator=evaluator,
                              numFolds=5)

    cvModel = crossval.fit(training_data)

    prediction = cvModel.transform(test_data)

    accuracy = evaluator.evaluate(prediction)
    output_str = output_str + "SVM accuracy is: " + str(accuracy) + "\n"

    predictionandLabels = prediction.withColumn('label1', prediction["label"].cast("double")).select("prediction","label1").rdd
    metrics = BinaryClassificationMetrics(predictionandLabels)

    auroc = metrics.areaUnderROC
    aupr = metrics.areaUnderPR
    output_str = output_str + "SVM Area under ROC Curve: " + str(auroc) + "\n"
    output_str = output_str + "SVM Area under PR Curve: " + str(aupr) + "\n"

    return output_str


def logistic_reg(training_data, test_data, output_str):

    # Create initial LogisticRegression model
    lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)

    paramGrid = ParamGridBuilder() \
        .addGrid(lr.maxIter, [75, 100]) \
        .addGrid(lr.regParam, [0.1, 0.01, 0.001, 10]) \
        .build()

    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

    crossval = CrossValidator(estimator=lr,
                              estimatorParamMaps=paramGrid,
                              evaluator=evaluator,
                              numFolds=5)  # use 3+ folds in practice

    # Run cross-validation, and choose the best set of parameters.
    cvModel = crossval.fit(training_data)

    predictionslr = cvModel.transform(test_data)

    # Evaluate model
    accuracy = evaluator.evaluate(predictionslr)
    output_str = output_str + "Logistic Regression accuracy is: " + str(accuracy) + "\n"

    predictionandLabels = predictionslr.withColumn('label1', predictionslr["label"].cast("double")).select("prediction","label1").rdd
    metrics = BinaryClassificationMetrics(predictionandLabels)

    auroc = metrics.areaUnderROC
    aupr = metrics.areaUnderPR
    output_str = output_str + "LR Area under ROC Curve: " + str(auroc) + "\n"
    output_str = output_str + "LR Area under PR Curve: " + str(aupr) + "\n"

    return output_str


### end of section for different models ###


def main():

    # Creating Spark context object
    sc = SparkContext(appName="Fake Opinion Detector")
    spark = SparkSession(sc)

    # Loading dataset files
    meta_data = sc.textFile(sys.argv[1]).map(lambda x: x.split("\t")).toDF(["user_id","prod_id","rating","label","date"])
    review_data = sc.textFile(sys.argv[2]).map(lambda x: x.split("\t")).toDF(["user_id","prod_id","date","review"])

    # Creating a dataframe containing review data and labels by joining meta_data and review_data df
    cf = meta_data.join(review_data, (meta_data.user_id==review_data.user_id) & (meta_data.prod_id==review_data.prod_id) & (meta_data.date==review_data.date))


    ##### Pre Processing #####
    class_balancedDf = pre_processing(cf)


    ##### Feature Engineering #####
    data = feature_engineering(class_balancedDf)

    # Split dataset randomly into Training and Test sets. Set seed for reproducibility
    (trainingData, testData) = data.randomSplit([0.7, 0.3], seed = 100)

    # caching data
    trainingData.cache()
    testData.cache()

    output_str = ""

    ##### Naive Bayes #####
    nb_str = naive_bayes(trainingData, testData, output_str)
    output_str = output_str + nb_str

    ##### Random Forest #####
    rf_str = random_forest(trainingData, testData, output_str)
    output_str = output_str + rf_str

    ##### Gradient Boosting #####
    gbt_str = gradient_boosting(trainingData, testData, output_str)
    output_str = output_str + gbt_str

    ##### SVM #####
    svm_str = svm(trainingData, testData, output_str)
    output_str = output_str + svm_str

    ##### Logistic Regression #####
    lr_str = logistic_reg(trainingData, testData, output_str)
    output_str = output_str + lr_str

    # Saving output as text file
    outputrdd = sc.parallelize(output_str.split("\n\n"))
    outputrdd.saveAsTextFile(sys.argv[3])

if __name__ == "__main__":
    main()
