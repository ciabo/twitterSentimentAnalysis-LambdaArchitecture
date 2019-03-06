import fastlayer.cassandra.SentimentRepository;

import javax.swing.*;
import java.util.*;

import static java.lang.Thread.sleep;

public class ServingLayer implements Runnable {
    private List<String> keywords;
    private SentimentRepository repository = SentimentRepository.getInstance();
    private Map<String, Long> map = new HashMap<String, Long>();
    private Map<String, Long> treeMap;
    private DrawChart dc;
    private int time = 0;

    public ServingLayer() {
        this.keywords = utils.Utils.getKeywords();
    }

    private List<Integer> getCounts(Map<String, Long> map) {
        int trend1 = 0;
        int trend2 = 0;
        int trend3 = 0;
        int totalTweets1 = 0;
        int totalTweets2 = 0;
        int totalTweets3 = 0;

        List<Integer> trend = new ArrayList<Integer>();
        for (String key : map.keySet()) {
            if (key.contains(keywords.get(0))) {
                totalTweets1++;
                if (key.contains(Integer.toString(-1)))
                    trend1 -= map.get(key);
                else if (key.contains(Integer.toString(1)))
                    trend1 += map.get(key);
            } else if (key.contains(keywords.get(1))) {
                totalTweets2++;
                if (key.contains(Integer.toString(-1)))
                    trend2 -= map.get(key);
                else if (key.contains(Integer.toString(1)))
                    trend2 += map.get(key);
            } else if (key.contains(keywords.get(2))) {
                totalTweets3++;
                if (key.contains(Integer.toString(-1)))
                    trend3 -= map.get(key);
                else if (key.contains(Integer.toString(1)))
                    trend3 += map.get(key);
            }
        }
        if(totalTweets1 == 0)
            totalTweets1 = 1;
        if(totalTweets2 == 0)
            totalTweets2 = 1;
        if(totalTweets3 == 0)
            totalTweets3 = 1;
        trend.add(0, trend1/totalTweets1);
        trend.add(1, trend2/totalTweets2);
        trend.add(2, trend3/totalTweets3);
        return trend;
    }

    @Override
    public void run() {
        try {
            dc = new DrawChart("Sentiment trend", 0, 0, 0, 0);
            while (true) {
                for (int i = 0; i < keywords.size(); i++) {
                    for (int j = -1; j <= 1; j++) {
                        String sentiment = Integer.toString(j);
                        String keyword_sentiment = keywords.get(i) + " | " + sentiment;
                        long count = repository.selectCountFromKey("fasttable", keywords.get(i), j);
                        map.put(keyword_sentiment, count);
                    }
                }

                for (int i = 0; i < keywords.size(); i++) {
                    for (int j = -1; j <= 1; j++) {
                        String sentiment = Integer.toString(j);
                        String keyword_sentiment = keywords.get(i) + " | " + sentiment;
                        long count = repository.selectCountFromKey("batchtable", keywords.get(i), j);
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
