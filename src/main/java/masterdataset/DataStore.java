package masterdataset;

import com.backtype.hadoop.pail.Pail;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataStore {
    public static FileSystem configureHDFS() {
        FileSystem fs = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/user/ettore");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false"); // TODO: 22/02/19 MAYBE in a cluster with multiple nodes must be true
            conf.setBoolean("dfs.support.append", true);
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            System.out.println("Unable to configure fle system!");
        }
        return fs;
    }

    public static HdfsAdmin adminHDFS() throws URISyntaxException {
        HdfsAdmin dfsAdmin = null;
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/user/ettore");
            conf.set("dfs.permissions.enabled", "false");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false"); // TODO: 22/02/19 MAYBE in a cluster with multiple nodes must be true
            conf.setBoolean("dfs.support.append", true);
            dfsAdmin = new HdfsAdmin(new URI("hdfs://localhost:9000/user/ettore"), conf);
        } catch (IOException e) {
            System.out.println("Unable to configure fle system!");
        }
        return dfsAdmin;
    }

    public static void closeHDFS(FileSystem fs) {
        try {
            fs.close();
        } catch (IOException e) {
            System.out.println("----------Unable to close  file system----------");
        }
    }

    private static List readPail(Pail<Tweet> p) {
        List tweets = new ArrayList();
        for (Tweet t : p) {
            tweets.add(Arrays.asList(t.getDate() + "," + t.getTimestamp() + "," + t.getTweet()));
        }
        return tweets;
    }

    public static void writeTweet(final Pail<Tweet> tweetPail, final String date, final String timestamp, final String tweet) throws IOException {
        Pail.TypedRecordOutputStream out = tweetPail.openWrite();
        out.writeObject(new Tweet(date, timestamp, tweet));
        out.close();
    }

    public static List readTweet(String path) throws IOException {
        Pail<Tweet> tweetPail = new Pail<Tweet>(path);
        return readPail(tweetPail);
    }

    private static void appendTweet(Pail src, Pail dst) throws IOException {
        dst.absorb(src);
        dst.consolidate();
    }

    public static List ingestPail(Pail tweetPail, Pail newPail, FileSystem fs, String test) throws IOException {
        fs.delete(new Path("/user/ettore/" + test + "pail/tweet/swa"), true);
        fs.mkdirs(new Path("/user/ettore/" + test + "pail/tweet/swa"));

        Pail<Tweet> snapShot = newPail.snapshot("hdfs://localhost:9000/user/ettore/" + test + "pail/tweet/swa/newData");
        List tweets = readPail(snapShot);
        appendTweet(newPail, tweetPail);
        newPail.deleteSnapshot(snapShot);
        return tweets;
    }
}