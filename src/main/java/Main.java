import com.backtype.hadoop.pail.Pail;
import com.datastax.driver.core.Session;
import fastlayer.storm.CountBolt;
import fastlayer.storm.SentimentBolt;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import masterdataset.Tweet;
import masterdataset.TweetStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import static masterdataset.DataStore.ingestPail;
import static masterdataset.DataStore.readTweet;
import static masterdataset.DataStore.writeTweet;
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

//        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("tweet_spout", new TweetSpout(), 1);
//        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
//        builder.setBolt("count_bolt", new CountBolt(),4).fieldsGrouping("sentiment_bolt",new Fields("keyword","sentiment"));
//        LocalCluster cluster = new LocalCluster();
//        Config conf = new Config();
//        conf.setDebug(true);
//        cluster.submitTopology("tweetp", conf, builder.createTopology());
//        sleep(1000000);
//        cluster.shutdown();

//        //tweet folder must be deleted at each execution
//        DataStore ds = new DataStore();
//        // Hadoop fs configuration
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://localhost:9000/user/ettore");
//        FileSystem fs = FileSystem.get(conf);
//        if (fs.exists(new Path("hdfs://localhost:9000/user/ettore/tweet/data")) &&
//                fs.exists(new Path("hdfs://localhost:9000/user/ettore/tweet/newData"))) {
//
//            fs.delete(new Path("hdfs://localhost:9000/user/ettore/tweet"), true);
//        }
//
//
//        String path = "hdfs://localhost:9000/user/ettore/tweet/data";
//        Pail tweetPail = Pail.create(path, new TweetStructure());
//        writeTweet(tweetPail, "Team Giannis", 1502019, 192133);
//        readTweet(path);
//
//        String newpath = "hdfs://localhost:9000/user/ettore/tweet/newData";
//        Pail newPail = Pail.create(newpath, new TweetStructure());
//        writeTweet(newPail, "This isn't good", 13022019, 155849);
//        readTweet(newpath);
//
//        System.out.println("Data folder: ");
//        ingestPail(tweetPail, newPail);
//        readTweet(path);

//        DataStore ds = new DataStore();
//        ds.compressPail(); // native-hadoop code error. The files are generated properly. Maybe a local error. Maybe unuseful.
//        ds.writeCompressedTweet("./tweet/compressed", "Ciao mondo", 18022019, 185344);
//
//        File file = new File("./db.txt");
//        FileInputStream fis = new FileInputStream(file);
//        InputStreamReader isr = new InputStreamReader(fis);
//        BufferedReader br = new BufferedReader(isr);
//        while (br.readLine() != null) {
//            String[] wrd = br.readLine().split(",");
//            ds.writeTweet(tweetPail, path, wrd[2], Integer.parseInt(wrd[0]), Integer.parseInt(wrd[1]));
//        }
//        br.close();

        MDatasetQuery mq = new MDatasetQuery();
        List tweet = Arrays.asList(Arrays.asList("Go gsw"),
                Arrays.asList("Shame!"),
                Arrays.asList("Tomorrow will be a good day"),
                Arrays.asList("Tomorrow apple will die"),
                Arrays.asList("Today google shows a new product"),
                Arrays.asList("CEO of microsoft is Bill Gates"),
                Arrays.asList("New microsoft update is available"),
                Arrays.asList("Jcascalog it's wonderful!"),
                Arrays.asList("apple it's wonderful!"));
        MDatasetQuery.tweetProcessing(tweet);
    }
}

