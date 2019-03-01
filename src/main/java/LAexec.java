import fastlayer.cassandra.SentimentRepository;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import org.apache.hadoop.fs.Path;
import org.apache.kerby.kerberos.kerb.crypto.util.Md4;
import utils.Utils;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LAexec {
    private List batch = new ArrayList();
    private List fast = new ArrayList();
    private MDatasetQuery mq = new MDatasetQuery();

    public LAexec(MDatasetQuery mq) {
        this.mq = mq;
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

    public void executeLA(FileSystem fs, TweetSpout spout) throws IOException, InterruptedException {

        int numFile = spout.getMainFolder() ? 1 : 2;
        if (DataStore.readFromHdfs(fs, "tweet/newData/newTweet" + numFile + ".txt").size() != 0) {
            fs.createSnapshot(new Path("tweet"),"culosnaphot");
            spout.changeFolder();
            List tweets = DataStore.readFromHdfs(fs, "tweet/newData/newTweet" + numFile + ".txt");
            System.out.println("\n" + tweets.size() + " tweets sended from " + numFile + ": " + tweets + "\n");
            mq.tweetProcessing(tweets);
            DataStore.createAppendHDFS(fs, "tweet/batchTweet/tweet.txt", tweets);
            DataStore.deleteFromHdfs(fs, "tweet/newData/newTweet" + numFile + ".txt");
            tweets.clear();
        }
    }

    public void recomputeBatch(SentimentRepository repository, FileSystem fs) {
        repository.deleteTable("batchtable");
        mq.setandCreateSentimentRepo(repository, "batchtable");
        List tweets = DataStore.readFromHdfs(fs, "tweet/batchTweet/tweet.txt");
        mq.tweetProcessing(tweets);
    }
}
