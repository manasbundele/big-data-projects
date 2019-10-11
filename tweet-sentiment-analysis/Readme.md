# Tweet Processing & Classification using Pipelines

We worked with a set of Tweets about US airlines and examined their sentiment polarity.
More details about the dataset is available at:
https://www.kaggle.com/crowdflower/twitter-airline-sentiment.

Our aim was to learn to classify Tweets as either “positive”,
“neutral”, or “negative” by using logistic regression classifier and pipelines for pre-processing
and model building. The code was able to run on AWS using a jar file created using IntelliJ.

## Implementation Overview

1. Pre-Processing:
    * Stop Word Removal
    * Tokenize
    * Term Hashing
    * Label Conversion
  
2. Model Creation: Logistic Regression model
3. Hyperparameter tuning using ParameterGridBuilder and Crossvalidation
4. Model Testing

All the above steps were implemented using a pipeline in spark.



