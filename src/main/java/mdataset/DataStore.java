package mdataset;

import com.backtype.hadoop.pail.Pail;

import java.io.IOException;

public class DataStore {
    public void writeTweet(Pail<Tweet> tweetPail, String path, String tweet, int date, int timestamp) throws IOException {
        Pail.TypedRecordOutputStream out = tweetPail.openWrite();
        out.writeObject(new Tweet(tweet, date, timestamp));
        out.close();
    }

    public void readTweet(Pail<Tweet> tweetPail, String path) throws IOException {
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
}
