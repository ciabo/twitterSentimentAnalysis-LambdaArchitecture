import com.backtype.hadoop.pail.Pail;
import junit.framework.TestCase;
import masterdataset.DataStore;
import masterdataset.TweetStructure;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PailTest {
    private static String path = "hdfs://localhost:9000/user/ettore/pail/tweet/data";
    private static String newpath = "hdfs://localhost:9000/user/ettore/pail/tweet/newData";
    private static Pail tweetPail;
    private static Pail newPail;
    private static FileSystem fs;
    private static List<String> fulltweetB;
    private static List<String> fulltweetF;

    @BeforeClass
    public static void setUp() throws IOException {
        fulltweetB = Arrays.asList("02032019", "120822", "Sunny ddaaaaayyyy!!");
        fulltweetF = Arrays.asList("04032018", "121333", "So sad this situation");
        fs = DataStore.configureHDFS();
        fs.delete(new Path("/user/ettore/pail"), true);
        tweetPail = Pail.create(path, new TweetStructure());
        newPail = Pail.create(newpath, new TweetStructure());
    }

    @AfterClass
    public static void tearDown() throws IOException {
        fs.delete(new Path("/user/ettore/pail"), true);
    }

    @Test
    public void testBatchPail() throws IOException {
        DataStore.writeTweet(tweetPail, "1502019", "192133", "Team Giannis");
        DataStore.writeTweet(tweetPail, "01032019", "121133", "Team Ettore");
        DataStore.readTweet(path);
    }

    @Test
    public void testBatchPailList() throws IOException {
        DataStore.writeTweet(tweetPail, fulltweetB.get(0), fulltweetB.get(1), fulltweetB.get(2));
        DataStore.readTweet(path);
    }

    @Test
    public void testNewDataPail() throws IOException {
        DataStore.writeTweet(newPail, "13022019", "155849", "Team Lebron");
        DataStore.writeTweet(newPail, "01032019", "140547", "Team Luca");
        DataStore.readTweet(newpath);
    }

    @Test
    public void testNewDataPailList() throws IOException {
        DataStore.writeTweet(newPail, fulltweetF.get(0), fulltweetF.get(1), fulltweetF.get(2));
        DataStore.readTweet(newpath);
    }

    @Test
    public void testIngestion() throws IOException {
        DataStore.ingestPail(tweetPail, newPail, fs); // It uses Map Reduce
        System.out.println("\nData folder: ");
        DataStore.readTweet(path);
        System.out.print("\nNew data folder: ");
        DataStore.readTweet(newpath);
    }
}
