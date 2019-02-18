package masterdataset;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
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

    public void appendTweet(Pail src, Pail dst) throws IOException {
        dst.absorb(src);
        dst.consolidate();
    }
//    public void ingestPail(Pail masterPail, Pail newDataPail) throws IOException {
//        FileSystem fs = new FileSystem.get(new Configuration());
//        fs.delete(new Path("./tweet/swa"), true); // folder to store the snapshot of new data folder
//        fs.mkdirs(new Path("./tweet/swa"));
//
//        Pail snapshotPail = newDataPail.snapshot("/tweet/swa/newDatasnapshot");
//
//    }

    public void compressPail() throws IOException {
        Map<String, Object> options = new HashMap<String, Object>();
        options.put(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
        options.put(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK);
        TweetStructure struct = new TweetStructure();
        Pail compressed = Pail.create("./tweet/compressed", new PailSpec("SequenceFile", options, struct));
    }

    public void ingestPail(final Pail masterPail, final Pail newDataPail) throws IOException {
        try {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hduser");

            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {

                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://localhost:9000/user/hduser");
                    conf.set("hadoop.job.ugi", "hduser");

                    FileSystem fs = FileSystem.get(conf);
                    fs.delete(new Path("./tweet/swa"), true); // folder to store the snapshot of new data folder
                    fs.mkdirs(new Path("./tweet/swa"));

                    Pail snapshotPail = newDataPail.snapshot("/tweet/swa/newDataSnapshot");
                    appendTweet(newDataPail, masterPail);
                    newDataPail.deleteSnapshot(snapshotPail);
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
//        //configuration setting for hadoop fs. Maybe work maybe not!

    }
}
