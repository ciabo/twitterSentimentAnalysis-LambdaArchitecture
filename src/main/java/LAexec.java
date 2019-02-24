import fastlayer.cassandra.SentimentRepository;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import org.apache.kerby.kerberos.kerb.crypto.util.Md4;
import utils.Utils;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Thread.sleep;

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
        // lo spout mi spara i tweet e me li salvo in un file sul dfs <- fatto
        // faccio uno snapshot del file lo salvo nel file tweet.txt e
        // ricalcolo i tweet con cascalog(tweetProcessing). Una volta
        // fatto lo snapshot posso cancellare i tweet in questione dal
        // file newTweet.txt.
        // Passato un certo tempo faccio il drop della batchtable e
        // ricalcolo il risultato su tutti i tweet in tweet.txt

        int numFile = spout.getnumFile();
        if (DataStore.readFromHdfs(fs, "tweet/newData/newTweet" + numFile + ".txt").size() != 0) {
//            spout.setNumFile(spout.getnumFile() == 1 ? 2 : 1);
            spout.setSnap(!spout.getSnap());
            List tweets = DataStore.readFromHdfs(fs, "tweet/newData/newTweet" + numFile + ".txt");
            System.out.println("Tweet sended to batch from file " + spout.getnumFile() + numFile + ":");
            System.out.println(tweets);
            mq.tweetProcessing(tweets);
            sleep(8000);
            DataStore.createAppendHDFS(fs, "tweet/batchTweet/tweet.txt", tweets);
            DataStore.deleteFromHdfs(fs, "tweet/newData/newTweet" + numFile + ".txt");
//            spout.setSnap(!spout.getSnap());
        }
    }

    public void recomputeBatch(SentimentRepository repository, FileSystem fs) {
        repository.deleteTable("batchtable");
        mq.setandCreateSentimentRepo(repository, "batchtable");
        List tweets = DataStore.readFromHdfs(fs, "tweet/batchTweet/tweet.txt");
        mq.tweetProcessing(tweets);
    }
}
