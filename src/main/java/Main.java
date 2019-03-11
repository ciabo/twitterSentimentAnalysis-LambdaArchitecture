
import backtype.storm.LocalCluster;
import fastlayer.cassandra.SentimentRepository;
import utils.Utils;

import java.io.*;
import java.util.Scanner;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {

        System.out.println("Insert 3 keywords (insert stop to break): ");
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
        int k = 1;
        while (k < 10000) {
            la.executeLA(init.getFs());
            sleep(15000);
            if (k % 10 == 0) {
                System.out.println("Recomputing all the batch");
                la.recomputeBatch(SentimentRepository.getInstance());
            }
            k++;
        }
        cluster.shutdown();

    }
}

