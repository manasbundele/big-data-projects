# Spark Streaming with Twitter and Kafka

The aim of this project was to create a Spark Streaming application that will continuously read data from
Twitter about a topic. These Twitter feeds will be analyzed for their sentiment, and then analyzed
using ElasticSearch. To exchange data between these two, Kafka will be used as a broker. 

Sentiment Analyzer: Stanford CoreNLP, https://github.com/clulab/processors/tree/master/corenlp

## Implementation Details

1. Created a Twitter application using Spark and Scala
2. The application would perform search about a topic and gather Tweets related to it. 
3. The next step is sentiment evaluation of the Tweets. The sentiment evaluation would happen continuously using a stream approach. At the end of
every window, a message containing the sentiment would be sent to Kafka through a topic.
4. Configured Logstash, Elasticsearch, and Kibana to read from the topic and set up visualization of sentiment.
5. To start elasticsearch: ./elasticsearch
6. To start kibana: ./kibana
7. To start logstash: bin/logstash -f logstash-simple.conf
8. Go to http://localhost:5601 and use Kibana to visualize your data in real-time. You will have to search for the appropriate topic index, which is
"YourTopic-index" in the logstash-simple.conf.

## Results

We searched for the tweets related to topic 'cricket' and analyzed its sentiment over a period of 4 hours.

Description of chart: The bar chart and kibana visualization file is attached. In the bar chart, the Y-axis refer to count of tweets and the X-axis has sentiments plotted. Sentiments vary from 0 to 4 with 0 being “very negative”, 1 as “negative”, 2 as “neutral”, 3 as “positive” and 4 as “very positive”. For each sentiment, we aggregated the values for a period of 30 minutes and displayed it in the chart with different color bars. The top right corner shows time slots for the different color bars.

This visualisation can then be used to analyze the change of emotions for a given topic at a given time.
