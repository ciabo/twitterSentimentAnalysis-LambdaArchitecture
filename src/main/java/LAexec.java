import com.backtype.hadoop.pail.Pail;
import fastlayer.cassandra.SentimentRepository;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import masterdataset.TweetStructure;
import utils.Utils;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LAexec {
    private List batch = new ArrayList();
    private List fast = new ArrayList();
    private MDatasetQuery mq;
    private Pail tweetPail;
    private Pail newTweetPail;
    private static String test = "";
    private String newpath = "hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/newData";
    private String batchPath;

    public LAexec(MDatasetQuery mq, String batchTweet) {
        this.mq = mq;
        this.batchPath = batchTweet;
        try {
            tweetPail = Pail.create(batchPath, new TweetStructure());
            newTweetPail = Pail.create(newpath, new TweetStructure());
        } catch (IOException e) {
            System.out.println("Unable to create bact tweet Pail");
        }
    }

    public void startLA(int endbatch, String filename) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
//            BufferedWriter bwB = new BufferedWriter(new FileWriter("dbBatch.txt"));
//            BufferedWriter bwF = new BufferedWriter(new FileWriter("dbFast15.txt"));
            String line;
            int count = 0;
            while ((line = br.readLine()) != null && count < 15) {
                String[] split = line.split(",");
                String tweet = Utils.createTweet(Arrays.copyOfRange(split, 2, split.length));
                if (count < endbatch) {
                    batch.add(Arrays.asList(tweet));
//                    bwB.append(tweet+"\n");
                } else {
                    fast.add(Arrays.asList(tweet));
//                    bwF.append(tweet+"\n");
                }
                count++;
            }
//            bwB.close();
//            bwF.close();
        } catch (IOException e) {
            System.out.println("Error with file " + filename);
        }
    }

    public List<String> getBatch() {
        return batch;
    }

    public List<String> getFast() {
        return fast;
    }

    public static void setTest(String test) {
        LAexec.test = test;
    }

    public void executeLA(FileSystem fs) throws IOException {
        List tweets = new ArrayList();
        if (DataStore.readTweet("hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/newData").size() != 0) {
            tweets = DataStore.ingestPail(tweetPail, newTweetPail, fs, test);
            mq.tweetProcessing(tweets);
        }
    }

    public void recomputeBatch(SentimentRepository repository, FileSystem fs) throws IOException {
        repository.deleteTable("batchtable");
        List tweets = DataStore.readTweet("pail/tweet/batchTweet");
        mq.tweetProcessing(tweets);
    }
}
