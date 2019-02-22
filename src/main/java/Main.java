import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fastlayer.storm.CountBolt;
import fastlayer.storm.SentimentBolt;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {
//        CassandraConnector client = new CassandraConnector();
//        client.connect("127.0.0.1", null);
//        Session session = client.getSession();
//        String keyspaceName = "tweetSentimentAnalysis";
//        KeyspaceRepository schemaRepository = new KeyspaceRepository(session);
//        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);
//        schemaRepository.useKeyspace(keyspaceName);
//        //create the table
//        SentimentRepository db = new SentimentRepository(session);
//        db.createTable();
//        db.updateCount("apple", -1, 15);
//        db.updateCount("google", 1, 6);
//        db.updateCount("microsoft", -1, 4);
//        int count = db.selectCountFromKey("apple", -1);
//        int a =1;

        FileSystem fs = DataStore.configureHDFS();
        String filePath = "tweet/newData/newTweet.txt";
        System.out.println(DataStore.readFromHdfs(fs, filePath));
//        DataStore.deleteFromHdfs(fs, filePath);
        System.out.println(DataStore.readFromHdfs(fs, filePath));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweet_spout", new TweetSpout(), 1);
        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
        builder.setBolt("count_bolt", new CountBolt(), 4).fieldsGrouping("sentiment_bolt", new Fields("keyword", "sentiment"));
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        cluster.submitTopology("tweetp", conf, builder.createTopology());
        sleep(1000000);
        cluster.shutdown();
        System.out.println(DataStore.readFromHdfs(fs, "tweet/newData/newTweet.txt"));

//        MDatasetQuery mq = new MDatasetQuery();
//
//        CassandraConnector client = new CassandraConnector();
//        client.connect("127.0.0.1", null);
//        mq.setSentimentRepo(new SentimentRepository(client.getSession()));
//
//        KeyspaceRepository keyspace = new KeyspaceRepository(client.getSession());
//        keyspace.createKeyspace("sentimentAnalysis", "SimpleStrategy", 1);
//        keyspace.useKeyspace("sentimentAnalysis");
//
//        List tweet = Arrays.asList(Arrays.asList("Go gsw"),
//                Arrays.asList("Shame!"),
//                Arrays.asList("Tomorrow will be a good day"),
//                Arrays.asList("Tomorrow apple will die"),
//                Arrays.asList("Today google shows a new product"),
//                Arrays.asList("CEO of microsoft is Bill Gates"),
//                Arrays.asList("New microsoft update is available"),
//                Arrays.asList("Jcascalog it's wonderful!"),
//                Arrays.asList("apple it's wonderful!"));
//        mq.tweetProcessing(tweet, "batchtable");
//        client.close();

//        LAexec la = new LAexec("db.txt");
////        la.startLA((int) (Utils.countFileLines("db.txt") * 0.7));
//        la.startLA(0);
//        List batch = la.getBatch();
//        List fast = la.getFast();
    }
}
