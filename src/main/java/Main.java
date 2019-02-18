import com.backtype.hadoop.pail.Pail;
import com.datastax.driver.core.Session;
import fastlayer.storm.CountBolt;
import fastlayer.storm.SentimentBolt;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import masterdataset.TweetStructure;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] argv) throws IOException {

//        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("tweet_spout", new TweetSpout(), 4);
//        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
//        builder.setBolt("count_bolt", new CountBolt(),4).fieldsGrouping("sentiment_bolt",new Fields("keyword"));
//        LocalCluster cluster = new LocalCluster();
//        Config conf = new Config();
//        conf.setDebug(true);
//        cluster.submitTopology("tweetp", conf, builder.createTopology());
//        cluster.shutdown();

        String path = "./tweet/data"; //tweet folder must be deleted at each execution
        Pail tweetPail = Pail.create(path, new TweetStructure());
        DataStore ds = new DataStore();
        ds.writeTweet(tweetPail, path, "Team Giannis", 1502019, 192133);
        ds.readTweet(tweetPail, path);
        String newpath = "./tweet/newdata";
        Pail newPail = Pail.create(newpath, new TweetStructure());
        ds.writeTweet(newPail, newpath, "New Tweet", 18022019, 185849);
        ds.appendTweet(newPail, tweetPail);
        ds.readTweet(tweetPail, path);

        ds.ingestPail(tweetPail, newPail);

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

//        MDatasetQuery mq = new MDatasetQuery();
//        List tweet = Arrays.asList(Arrays.asList("Go gsw"),
//                Arrays.asList("Shame!"),
//                Arrays.asList("Tomorrow will be a good day"),
//                Arrays.asList("Tomorrow apple will die"),
//                Arrays.asList("Today google shows a new product"),
//                Arrays.asList("CEO of microsoft is Bill Gates"),
//                Arrays.asList("New microsoft update is available"),
//                Arrays.asList("Jcascalog it's wonderful!"));
//        MDatasetQuery.tweetProcessing(tweet);
    }
}
