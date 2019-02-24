import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.datastax.driver.core.Session;
import fastlayer.cassandra.CassandraConnector;
import fastlayer.cassandra.KeyspaceRepository;
import fastlayer.cassandra.SentimentRepository;
import fastlayer.storm.CountBolt;
import fastlayer.storm.SentimentBolt;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import org.apache.hadoop.fs.FileSystem;
import utils.Utils;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {
//        System.out.println("Drop tables? (true/false)");
//        Scanner sc = new Scanner(System.in);
//        boolean drop = sc.nextBoolean();
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
        String filePath = "/user/ettore/tweet/batchTweet/tweet.txt";
        DataStore.deleteFromHdfs(fs, filePath);

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
        LAexec la = new LAexec(mq);
        for (int i = 0; i < 4; i++) { // 15 tweets in total, each iteration consumes 4 tweets, 3/4 iterations are enough
            la.executeLA(fs, spout);
            sleep(15000); //almost 4 tweets
        }

        cluster.shutdown();
        System.out.println("\nNew tweets stored in dfs:");
        List filecontent = DataStore.readFromHdfs(fs, filePath);
        for (Object o : filecontent)
            System.out.println(o);
        System.out.println("Number of tweets: " + filecontent.size());

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
