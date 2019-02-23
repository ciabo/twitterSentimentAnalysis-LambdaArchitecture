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

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {
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

        //generate tweet to jCascalog
        List tweet = Utils.createListofTweet("dbFast15.txt");

        // create batchlayer table
        MDatasetQuery mq = new MDatasetQuery();
        SentimentRepository dbB = new SentimentRepository(session);
        mq.setandCreateSentimentRepo(dbB, "batchtable");

        // put tweet processing in batchtable
        mq.tweetProcessing(tweet);

        // configure and set file to store new fastlayer's tweet
        FileSystem fs = DataStore.configureHDFS();
        String filePath = "tweet/newData/newTweet.txt";
//        System.out.println(DataStore.readFromHdfs(fs, filePath));
        DataStore.deleteFromHdfs(fs, filePath);
//        System.out.println(DataStore.readFromHdfs(fs, filePath));

        // storm init
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet_spout", new TweetSpout(), 1);
        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
        builder.setBolt("count_bolt", new CountBolt(), 4).fieldsGrouping("sentiment_bolt", new Fields("keyword", "sentiment"));
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);

        // storm execution
        cluster.submitTopology("tweetp", conf, builder.createTopology());
        sleep(10000);
        cluster.shutdown();
        System.out.println("\nNew tweet stored in dfs:");
        System.out.println(DataStore.readFromHdfs(fs, filePath));

        ServingLayer servingLayer = new ServingLayer(dbF);
        String[] keywords = {"google", "apple", "microsoft"};

        Map<String, Integer> results = servingLayer.getResults(keywords);
        System.out.println("QUERY RESULTS");
        System.out.println("------------------");
        for (String key : results.keySet())
            System.out.println(key + " | " + results.get(key));
        System.out.println("------------------");

        // drop table
        dbF.deleteTable("fasttable");
        dbB.deleteTable("bathctable");

    }
}
