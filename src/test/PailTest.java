

import com.backtype.hadoop.pail.Pail;
import batchlayer.pail.DataStore;
import batchlayer.pail.TweetStructure;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;
import utils.Utils;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

public class PailTest {
    private static String path = "hdfs://localhost:9000/user/ettore/testpail/tweet/data";
    private static String newpath = "hdfs://localhost:9000/user/ettore/testpail/tweet/newData";
    private static Pail tweetPail;
    private static Pail newPail;
    private static FileSystem fs;
    private static String line1;
    private static String line2;

    @BeforeClass
    public static void setUp() throws IOException {
        line1 = "02032019,120822,Sunny ddaaaaayyyy!!";
        line2 = "04032018,121333,So sad this situation";
        fs = DataStore.configureHDFS();
        fs.delete(new Path("/user/ettore/testpail"), true);
        tweetPail = Pail.create(path, new TweetStructure());
        newPail = Pail.create(newpath, new TweetStructure());
    }

    @AfterClass
    public static void tearDown() throws IOException {
        fs.delete(new Path("/user/ettore/testpail"), true);
    }

    @Test
    public void testBatchPail() throws IOException {
        DataStore.writeTweet(tweetPail, "15022019", "192133", "Team Giannis");
        DataStore.writeTweet(tweetPail, "01032019", "121133", "Team Ettore");
        List tweets = DataStore.readTweet(path);
        Utils.printLisofList(tweets);
        assertEquals(2, tweets.size());
    }

    @Test
    public void testBatchPailList() throws IOException {
        List<String> fulltweetB = Utils.generateTweet(line1);
        DataStore.writeTweet(tweetPail, fulltweetB.get(0), fulltweetB.get(1), fulltweetB.get(2));
        List tweets = DataStore.readTweet(path);
        Utils.printLisofList(tweets);
        assertEquals(3, tweets.size());
    }

    @Test
    public void testNewDataPail() throws IOException {
        DataStore.writeTweet(newPail, "13022019", "155849", "Team Lebron");
        DataStore.writeTweet(newPail, "01032019", "140547", "Team Luca");
        List tweets = DataStore.readTweet(newpath);
        Utils.printLisofList(tweets);
        assertEquals(2, tweets.size());
    }

    @Test
    public void testNewDataPailList() throws IOException {
        List<String> fulltweetF = Utils.generateTweet(line2);
        DataStore.writeTweet(newPail, fulltweetF.get(0), fulltweetF.get(1), fulltweetF.get(2));
        List tweets = DataStore.readTweet(newpath);
        Utils.printLisofList(tweets);
        assertEquals(3, tweets.size());
    }

    @Test
    public void testIngestion() throws IOException {
        DataStore.ingestPail(tweetPail, newPail, fs, "test"); // It uses Map Reduce
        System.out.println("\nData folder: ");
        List tweets = DataStore.readTweet(path);
        Utils.printLisofList(tweets);
        assertEquals(6, tweets.size());
        System.out.print("\nNew data folder: ");
        List newTweet = DataStore.readTweet(newpath);
        Utils.printLisofList(newTweet);
        assertEquals(0, newTweet.size());
    }
}
