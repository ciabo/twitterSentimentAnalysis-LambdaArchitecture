import com.backtype.hadoop.pail.Pail;
import mdataset.DataStore;
import mdataset.TweetStructure;

import java.io.*;

public class Main {
    public static void main(String[] argv) throws IOException {
        String path = "./tweet/data"; //tweet folder must be deleted at each execution
        Pail tweetPail = Pail.create(path, new TweetStructure());
        DataStore ds = new DataStore();
        ds.writeTweet(tweetPail, path, "Team Giannis", 1502019, 192133);
        ds.readTweet(tweetPail, path);

//
//        File file = new File("./db.txt");
//        FileInputStream fis = new FileInputStream(file);
//        InputStreamReader isr = new InputStreamReader(fis);
//        BufferedReader br = new BufferedReader(isr);
//        while (br.readLine() != null) {
//            String[] wrd = br.readLine().split(",");
//            ds.writeTweet(tweetPail, path, wrd[2], Integer.parseInt(wrd[0]), Integer.parseInt(wrd[1]));
//        }
//        br.close();
    }
}
