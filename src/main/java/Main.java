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
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {

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
        SentimentRepository db = SentimentRepository.getInstance(session);

        // drop table
        if (drop) {
            db.deleteTable("fasttable");
            db.deleteTable("batchtable");
            db.createTable("fasttable");
            db.createTable("batchtable");
        }

        // configure and set file to store(batch) new fastlayer's tweets
        FileSystem fs = DataStore.configureHDFS();
        fs.delete(new Path("/user/ettore/pail/tweet"), true);

        // storm init
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet_spout", new TweetSpout(), 1);
        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
        builder.setBolt("count_bolt", new CountBolt(), 4).fieldsGrouping("sentiment_bolt", new Fields("keyword", "sentiment"));
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, false);

        // storm execution
        cluster.submitTopology("tweetp", conf, builder.createTopology());

        MDatasetQuery mq = new MDatasetQuery();
        // put tweets processing in batchtable
        String batchpath = "hdfs://localhost:9000/user/ettore/pail/tweet/batchTweet";
        LAexec la = new LAexec(mq, batchpath);
        String[] keywords = {"google", "apple", "microsoft"};
        Thread t = new Thread(new ServingLayer(keywords));
        t.start();
        int k=0; //da fare: mettere un bel controllo sulla dimensione del file
        //certe volte dà errore quasi subito dicendo che non esiste un pail
        while(k<10000){
            la.executeLA(fs);
            sleep(15000); //almost 4 tweets
            k++;
        }

        cluster.shutdown();
        System.out.println("\nNew tweets stored in dfs:");
        List tweets = DataStore.readTweet(batchpath);
        System.out.println("Number of tweets: " + tweets.size());




    }
}

