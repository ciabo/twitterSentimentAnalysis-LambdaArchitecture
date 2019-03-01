package fastlayer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.backtype.hadoop.pail.Pail;
import com.backtype.support.Utils;
import masterdataset.DataStore;
import masterdataset.Tweet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TweetSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> records;
    private int dbcounter;
    private String newpath = "hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/newData";
    private Pail<Tweet> newTweetPail;
    private static String test = "";
    private static String numFileforTest = "";

    //open is called during initialization by storm and the SpoutOutputCollector is where the output will be sent
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            newTweetPail = new Pail<Tweet>(newpath);
        } catch (IOException e) {
            System.out.println("--------------New Pail not created---------------");
        }

        this.collector = collector;
        this.dbcounter = 0;
        String filename = "db" + numFileforTest + ".txt";
        this.records = new ArrayList<String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                records.add(line);
            }
            reader.close();
        } catch (Exception e) {
            System.err.format("Exception occurred trying to read '%s'.", filename);
            e.printStackTrace();
        }
    }

    //colled again during initialization to know how the tuples generated are structured
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public void nextTuple() {
        Utils.sleep(100);
        String line = records.get(dbcounter);
        if (dbcounter < records.size() - 1) {
            dbcounter++;
            try {
                System.out.println("Writing on newTweet");
                List<String> fulltweet = utils.Utils.generateTweet(line);
                DataStore.writeTweet(newTweetPail, fulltweet.get(0), fulltweet.get(1), fulltweet.get(2));
            } catch (IOException e) {
                System.out.println("Error while appending newTweet");
            }
            collector.emit(new Values(line));
        }
    }

    // Method for testing
    public static void setTest(String test, String check) {
        if (check.equals("test")) {
            TweetSpout.numFileforTest = test;
            TweetSpout.test = "test";
        }
    }
}
