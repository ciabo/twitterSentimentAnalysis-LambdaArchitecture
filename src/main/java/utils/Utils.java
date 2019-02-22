package utils;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;

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
}

