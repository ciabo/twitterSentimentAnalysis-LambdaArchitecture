import com.backtype.hadoop.pail.Pail;
import fastlayer.cassandra.SentimentRepository;
import fastlayer.storm.TweetSpout;
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
    private String newpath = "hdfs://localhost:9000/user/ettore/pail/tweet/newData";

    public LAexec(MDatasetQuery mq, String batchTweet) {
        this.mq = mq;
        try {
            tweetPail = Pail.create(batchTweet, new TweetStructure());
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

    public void executeLA(FileSystem fs) throws IOException {
        DataStore.ingestPail(tweetPail, newTweetPail, fs, "");
    }

    public void recomputeBatch(SentimentRepository repository, FileSystem fs) throws IOException {
        repository.deleteTable("batchtable");
        List tweets = DataStore.readTweet("pail/tweet/batchTweet");
        mq.tweetProcessing(tweets);
    }
}
