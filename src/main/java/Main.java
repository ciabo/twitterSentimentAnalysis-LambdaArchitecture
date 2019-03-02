
import backtype.storm.LocalCluster;
import masterdataset.DataStore;
import org.apache.hadoop.fs.Path;
import utils.Utils;

import java.io.*;
import java.util.List;

import static java.lang.Thread.sleep;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {

        //Pail, Cassandra, Filesystem and Storm init
        Init init = new Init();

        // put tweets processing in batchtable
        LAexec la = new LAexec(init.getMq());

        // storm execution
        LocalCluster cluster = init.getCluster();
        cluster.submitTopology("tweetp", init.getConf(), init.getBuilder().createTopology());

        // start ping pong
        String[] keywords = {"google", "apple", "microsoft"};
        Thread t = new Thread(new ServingLayer(keywords));
        t.start();
        int k = 0; //da fare: mettere un bel controllo sulla dimensione del file
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

