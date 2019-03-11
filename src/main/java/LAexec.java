import com.backtype.hadoop.pail.Pail;
import fastlayer.cassandra.SentimentRepository;

import batchlayer.pail.DataStore;
import batchlayer.jcascalog.MDatasetQuery;
import batchlayer.pail.TweetStructure;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.List;

public class LAexec {
    private MDatasetQuery mq;
    private Pail tweetPail;
    private Pail newTweetPail;
    private static String test = "";
    private String newpath = "hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/newData";
    private String batchPath = "hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/batchTweet";

    public LAexec(MDatasetQuery mq) {
        this.mq = mq;
        try {
            if (test.equals("test")) {
                this.newTweetPail = Pail.create(newpath, new TweetStructure());
                this.tweetPail = Pail.create(batchPath, new TweetStructure());
            } else {
                newTweetPail = new Pail(newpath);
                tweetPail = new Pail(batchPath);
            }
        } catch (IOException e) {
        }
    }

    public void executeLA(FileSystem fs) throws IOException {
        List tweets;
        if (DataStore.readTweet("hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/newData").size() != 0) {
            tweets = DataStore.ingestPail(tweetPail, newTweetPail, fs, test);
            mq.tweetProcessing(tweets);
        }
    }

    public void recomputeBatch(SentimentRepository repository) throws IOException {
        repository.deleteTable("batchtable");
        repository.createTable("batchtable");
        List tweets = DataStore.readTweet("hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/batchTweet");
        mq.tweetProcessing(tweets);
    }

    public static void setTest(String test) {
        LAexec.test = test;
    }
}
