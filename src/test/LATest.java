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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.Thread.sleep;

public class LATest {
    private static SentimentRepository db;
    private static FileSystem fs;
    private static LocalCluster cluster;
    private static String batchpath = "hdfs://localhost:9000/user/ettore/testpail/tweet/batchTweet";
    private static LAexec la;
    private long count;

    @BeforeClass
    public static void setUp() throws IOException {
        TweetSpout.setTest("15", "test");
        LAexec.setTest("test");

        //cassandra cluster init
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", null);
        Session session = client.getSession();

        // keyspace of batch and fast layer
        String keyspaceName = "tweetSentimentAnalysis";
        KeyspaceRepository schemaRepository = new KeyspaceRepository(session);
        schemaRepository.useKeyspace(keyspaceName);

        // create fastlayer table
        db = SentimentRepository.getInstance(session);

        // create table
        db.createTable("fasttable");
        db.createTable("batchtable");

        // configure and set file to store(batch) new fastlayer's tweets
        fs = DataStore.configureHDFS();
        fs.delete(new Path("/user/ettore/testpail/tweet"), true);

        // Object to process batch tweet
        MDatasetQuery mq = new MDatasetQuery();

        // init Lambda Architecture
        la = new LAexec(mq, batchpath);

        // storm init
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet_spout", new TweetSpout(), 1);
        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
        builder.setBolt("count_bolt", new CountBolt(), 4).fieldsGrouping("sentiment_bolt", new Fields("keyword", "sentiment"));
        cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);

        // storm execution
        cluster.submitTopology("tweetp", conf, builder.createTopology());
    }

    @AfterClass
    public static void tearDown() throws IOException {
        // drop table
        db.deleteTable("fasttable");
        db.deleteTable("batchtable");
        fs.delete(new Path("/user/ettore/testpail"), true);
    }

    @Test
    public void testPingPong() throws IOException, InterruptedException {
        // put tweets processing in batchtable
        for (int i = 0; i < 5; i++) { // 15 tweets in total, each iteration consumes 4 tweets, 3/4 iterations are enough
            la.executeLA(fs);
            sleep(15000); //almost 4 tweets
        }

        cluster.shutdown();
        System.out.println("\nNew tweets stored in dfs:");
        List tweets = DataStore.readTweet(batchpath);
        assertEquals(14, tweets.size());
    }

    @Test
    public void testServingLayer() {
        String[] keywords = {"google", "apple", "microsoft"};

        Map<String, Long> results = ServingLayer.getResults(keywords);
        Map<String, Long> treeMap = new TreeMap<String, Long>(results); // sort by key
        System.out.println("QUERY RESULTS");
        System.out.println("------------------");
        for (String key : treeMap.keySet())
            System.out.println(key + " | " + treeMap.get(key));
        System.out.println("------------------");
        for (String k : keywords) {
            for (int s = -1; s <= 1; s++) {
                count += db.selectCountFromKey("batchtable", k, s);
            }
        }
        assertEquals(9, count);
    }
}
