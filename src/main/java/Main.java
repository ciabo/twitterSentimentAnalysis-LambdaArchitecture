import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.backtype.hadoop.pail.Pail;
import com.datastax.driver.core.Session;
import fastlayer.cassandra.CassandraConnector;
import fastlayer.cassandra.KeyspaceRepository;
import fastlayer.cassandra.SentimentRepository;
import fastlayer.storm.CountBolt;
import fastlayer.storm.SentimentBolt;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import masterdataset.TweetStructure;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import utils.Utils;

import java.io.*;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException, URISyntaxException {

        boolean drop = true;

        //cassandra cluster init
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", null);
        Session session = client.getSession();

        // keyspace of batch and fast layer
        String keyspaceName = "tweetSentimentAnalysis";
        KeyspaceRepository schemaRepository = new KeyspaceRepository(session);
        schemaRepository.useKeyspace(keyspaceName);
        // create fastlayer table
        SentimentRepository dbF = new SentimentRepository(session);

        // create batchlayer table
        MDatasetQuery mq = new MDatasetQuery();
        SentimentRepository dbB = new SentimentRepository(session);
        mq.setandCreateSentimentRepo(dbB, "batchtable");

        // drop table
        if (drop) {
            dbF.deleteTable("fasttable");
            dbB.deleteTable("bathctable");
            dbF.createTable("fasttable");
            dbF.createTable("batchtable");
        }

        // configure and set file to store(batch) new fastlayer's tweets
        FileSystem fs = DataStore.configureHDFS();
        String filePath = "tweet/batchTweet/tweet.txt";
        fs.delete(new Path("/user/ettore/pail/tweet"), true);

        // storm init
        TopologyBuilder builder = new TopologyBuilder();
        TweetSpout spout = new TweetSpout();
        builder.setSpout("tweet_spout", spout, 1);
        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
        builder.setBolt("count_bolt", new CountBolt(), 4).fieldsGrouping("sentiment_bolt", new Fields("keyword", "sentiment"));
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);

        // storm execution
        cluster.submitTopology("tweetp", conf, builder.createTopology());

        // put tweets processing in batchtable
        String batchpath = "hdfs://localhost:9000/user/ettore/pail/tweet/batchTweet";
        LAexec la = new LAexec(mq, batchpath);
        for (int i = 0; i < 5; i++) { // 15 tweets in total, each iteration consumes 4 tweets, 3/4 iterations are enough
            la.executeLA(fs);
            sleep(15000); //almost 4 tweets
        }

        cluster.shutdown();
        System.out.println("\nNew tweets stored in dfs:");
        List tweets = DataStore.readTweet(batchpath);
        System.out.println("Number of tweets: " + tweets.size());

        ServingLayer servingLayer = new ServingLayer(dbF);
        String[] keywords = {"google", "apple", "microsoft"};

        Map<String, Integer> results = servingLayer.getResults(keywords);
        Map<String, Integer> treeMap = new TreeMap<String, Integer>(results); // sort by key
        System.out.println("QUERY RESULTS");
        System.out.println("------------------");
        for (String key : treeMap.keySet())
            System.out.println(key + " | " + treeMap.get(key));
        System.out.println("------------------");
    }
}

