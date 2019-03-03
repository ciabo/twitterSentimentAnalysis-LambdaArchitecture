import fastlayer.cassandra.SentimentRepository;

import javax.swing.*;
import java.util.*;

import static java.lang.Thread.sleep;

public class ServingLayer implements Runnable {
    private String[] keywords;
    private SentimentRepository repository = SentimentRepository.getInstance();
    private Map<String, Long> map = new HashMap<String, Long>();
    private Map<String, Long> treeMap;
    private DrawChart dc;
    private int time = 0;

    public ServingLayer(String[] keywords) {
        this.keywords = keywords;
    }

    private List<Integer> getCounts(Map<String, Long> map) {
        int trendA = 0;
        int trendG = 0;
        int trendM = 0;
        List<Integer> trend = new ArrayList<Integer>();
        for (String key : map.keySet()) {
            if (key.contains("apple")) {
                if (key.contains(Integer.toString(-1)))
                    trendA -= map.get(key);
                else if (key.contains(Integer.toString(1)))
                    trendA += map.get(key);
            } else if (key.contains("google")) {
                if (key.contains(Integer.toString(-1)))
                    trendG -= map.get(key);
                else if (key.contains(Integer.toString(1)))
                    trendG += map.get(key);
            } else if (key.contains("microsoft")) {
                if (key.contains(Integer.toString(-1)))
                    trendM -= map.get(key);
                else if (key.contains(Integer.toString(1)))
                    trendM += map.get(key);
            }
        }
        trend.add(0, trendA);
        trend.add(1, trendG);
        trend.add(2, trendM);
        return trend;
    }

    @Override
    public void run() {
        try {
            dc = new DrawChart("Sentiment trend", 0, 0, 0, 0);
            while (true) {
                for (int i = 0; i < keywords.length; i++) {
                    for (int j = -1; j <= 1; j++) {
                        String sentiment = Integer.toString(j);
                        String keyword_sentiment = keywords[i] + " | " + sentiment;
                        long count = repository.selectCountFromKey("fasttable", keywords[i], j);
                        map.put(keyword_sentiment, count);
                    }
                }

                for (int i = 0; i < keywords.length; i++) {
                    for (int j = -1; j <= 1; j++) {
                        String sentiment = Integer.toString(j);
                        String keyword_sentiment = keywords[i] + " | " + sentiment;
                        long count = repository.selectCountFromKey("batchtable", keywords[i], j);
                        long val = map.get(keyword_sentiment);
                        long newVal = val + count;
                        map.put(keyword_sentiment, newVal);
                    }
                }
                treeMap = new TreeMap<String, Long>(map); // sort by key
                System.out.println("QUERY RESULTS");
                System.out.println("------------------");
                for (String key : treeMap.keySet())
                    System.out.println(key + " | " + treeMap.get(key));
                System.out.println("------------------");
                try {
                    sleep(10000);
                } catch (InterruptedException e) {
                    System.out.println("could not sleep the thread");
                }

                // plot trend
                List<Integer> trend = getCounts(map);
                System.out.println("Apple: " + trend.get(0) + " Google: " + trend.get(1) + " Microsoft: " + trend.get(2));
                dc.update(trend.get(0), trend.get(1), trend.get(2), time += 10);
                dc.setSize(800, 400);
                dc.setLocationRelativeTo(null);
//            dc.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
                dc.setVisible(true);

                //cleaning of maps
                map.clear();
                treeMap.clear();
            }
        } finally {
            //cleaning of maps
            map.clear();
            treeMap.clear();
        }
    }
}
