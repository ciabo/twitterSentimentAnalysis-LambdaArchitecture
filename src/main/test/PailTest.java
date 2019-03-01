import com.backtype.hadoop.pail.Pail;
import junit.framework.TestCase;
import masterdataset.DataStore;
import masterdataset.TweetStructure;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;

import java.io.IOException;

public class PailTest {
    private static String path = "hdfs://localhost:9000/user/ettore/pail/tweet/data";
    private static String newpath = "hdfs://localhost:9000/user/ettore/pail/tweet/newData";
    private static Pail tweetPail;
    private static Pail newPail;
    private static FileSystem fs;

    @BeforeClass
    public static void setUp() throws IOException {
        fs = DataStore.configureHDFS();
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
        DataStore.readTweet(path);
    }

    @Test
    public void testNewDataPail() throws IOException {
        DataStore.writeTweet(newPail, "13022019", "155849", "Team Lebron");
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
