
import backtype.storm.LocalCluster;
import batchlayer.pail.DataStore;
import utils.Utils;

import java.io.*;
import java.util.List;
import java.util.Scanner;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {

        System.out.println("Insert keywords (insert stop to break): ");
        Scanner sc = new Scanner(System.in);
        while (true) {
            String word = sc.next();
            if (word.toLowerCase().equals("stop"))
                break;
            Utils.addKeyword(word.toLowerCase());
        }

        System.out.println(Utils.getKeywords());

        //Pail, Cassandra, Filesystem and Storm init
        Init init = new Init();

        // put tweets processing in batchtable
        LAexec la = new LAexec(init.getMq());

        // storm execution
        LocalCluster cluster = init.getCluster();
        cluster.submitTopology("tweetp", init.getConf(), init.getBuilder().createTopology());

        // start ping pong
        Thread t = new Thread(new ServingLayer());
        t.start();
        int k = 0; //Todo mettere un bel controllo sulla dimensione del file <-- non so a che serve.. usa countlines di Utils
        while (k < 10000) {
            la.executeLA(init.getFs());
            sleep(15000); //almost 4 tweets
            k++;
        }
        cluster.shutdown();

        // tweets copied from stream to batch
        System.out.println("\nNew tweets stored in dfs:");
        List<List<String>> tweets = DataStore.readTweet(init.getBatchPath());
        Utils.printLisofList(tweets);
        System.out.println("Number of tweets: " + tweets.size());
    }
}

