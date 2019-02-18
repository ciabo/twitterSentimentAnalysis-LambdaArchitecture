package masterdataset;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataStore {
    public void writeTweet(Pail<Tweet> tweetPail, String path, String tweet, int date, int timestamp) throws IOException {
        Pail.TypedRecordOutputStream out = tweetPail.openWrite();
        out.writeObject(new Tweet(tweet, date, timestamp));
        out.close();
    }

    public void writeCompressedTweet(String path, String tweet, int date, int timestamp) throws IOException {
        Pail pail = new Pail<Tweet>(path);
        Pail.TypedRecordOutputStream out = pail.openWrite();
        out.writeObject(new Tweet(tweet, date, timestamp));
        out.close();
    }

    public void readTweet(Pail<Tweet> tweetPail, String path) throws IOException {
        for (Tweet t : tweetPail) {
            System.out.println("Tweet: " + t.getTweet() + " Date: " + t.getDate() + " Time: " + t.getTimestamp());
        }
    }

    public void readCompressedTweet(String path) throws IOException {
        Pail<Tweet> tweetPail = new Pail<Tweet>(path);
        for (Tweet t : tweetPail) {
            System.out.println("Tweet: " + t.getTweet() + " Date: " + t.getDate() + " Time: " + t.getTimestamp());
        }
    }

    public void appendTweet(String src, String dst) throws IOException {
        Pail<Tweet> tweetOld = new Pail<Tweet>(dst);
        Pail<Tweet> tweetNew = new Pail<Tweet>(src);
        tweetOld.absorb(tweetNew);
        tweetOld.consolidate();
    }

    public void compressPail() throws IOException {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
        options.put(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK);
        TweetStructure struct = new TweetStructure();
        Pail compressed = Pail.create("./tweet/compressed", new PailSpec("SequenceFile", options, struct));
    }
}
