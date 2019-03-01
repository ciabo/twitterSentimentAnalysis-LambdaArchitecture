package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Utils {
    public static String createTweet(String[] array) {
        StringBuilder tweet = new StringBuilder();
        for (String str : array) {
            tweet.append(str);
        }
        return tweet.toString();
    }

    public static int countFileLines(String filename) {
        try {
            LineNumberReader lr = new LineNumberReader(new FileReader(filename));
            while (lr.skip(Long.MAX_VALUE) > 0) {
            }
            return lr.getLineNumber();
        } catch (IOException e) {
            System.out.println("Error counting lines");
            return -1;
        }
    }

    public static List createListofTweet(String filePath) {
        List batch = new ArrayList();
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            //BufferedWriter bwB = new BufferedWriter(new FileWriter("dbBatch.txt"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split(",");
                String tweet = Utils.createTweet(Arrays.copyOfRange(split, 2, split.length));
                batch.add(Arrays.asList(tweet));
//              bwB.append(tweet+"\n");

            }
//            bwB.close();
        } catch (IOException e) {
            System.out.println("Error generating list of tweet from file " + filePath);
        }
        return batch;
    }

    public static List generateTweet(String line) {
        String[] split = line.split(",");
        String tweet = Utils.createTweet(Arrays.copyOfRange(split, 2, split.length));
        List<String> fulltweet = Arrays.asList(split[0], split[1], tweet);
        return fulltweet;
    }
}

