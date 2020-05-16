# Twitter Sentiment Analysis using a Lambda Architecture
Sentiment Analysis is the process of determining whether a piece of writing (a tweet in our case) is positive, negative or
neutral. It can be used to identify the customer or follower’s attitude towards a brand.
Suppose you want to process all the tweet of the year (approximately 200 billion) until now. Distributing both data and computation over clusters of tens or hundreds of machines maybe is possible to process all the tweet in few hours, but the results would ignore all the tweets arrived during the computation and this, for some kind of requests, is not acceptable.

Lambda architecture is a data-processing architecture designed to handle massive quantities of data by taking advantage of both batch and stream-processing methods solving the previous problem.

## Software requirements
- Java JDK: open jdk 11
- Apache Hadoop 2.9.2
- Apache Storm 2.1.0
- HdfsSpout Apache Storm 2.1.0
- LingPipe 4.1.2

## Dataset
Sentiment140, 1.6 million tweets with annotated sentiment.

## Usage

At execution the software asks to choose three keywords. The analysis will be executed on the tweets that contains at least
one of the three keywords choosen.
Once the program start, auto-updating charts will show the trend for each keyword simulating the arrival of new tweets from the dataset.
![results](https://github.com/ciabo/twitterSentimentAnalysis-LambdaArchitecture/blob/master/trend.png)

Read the paper for more infos.
Credits to: Nathan Marz
