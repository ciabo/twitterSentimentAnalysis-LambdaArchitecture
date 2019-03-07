import fastlayer.cassandra.SentimentRepository;

import java.util.*;

import static java.lang.Thread.sleep;

public class ServingLayer implements Runnable {
    private List<String> keywords;
    private SentimentRepository repository = SentimentRepository.getInstance();
    private Map<String, Long> map = new HashMap<String, Long>();
    private Map<String, Long> treeMap;
    private DrawChart dc;
    private int time = 0;
    private float[] trend;
    private float[] totalTweets;

    public ServingLayer() {
        this.keywords = utils.Utils.getKeywords();
    }

    private float[] getCounts(Map<String, Long> map) {
        float[] trends = new float[3];
        trend = new float[3];
        totalTweets = new float[3];

        for (String key : map.keySet()) {
            if (key.contains(keywords.get(0))) {
                updateTrend(0, key);
            } else if (key.contains(keywords.get(1))) {
                updateTrend(1, key);
            } else if (key.contains(keywords.get(2))) {
                updateTrend(2, key);
            }
        }

        totalTweets[0] = totalTweets[0] == 0 ? 1 : totalTweets[0];
        totalTweets[1] = totalTweets[1] == 0 ? 1 : totalTweets[1];
        totalTweets[2] = totalTweets[2] == 0 ? 1 : totalTweets[2];

        trends[0] = trend[0] / totalTweets[0];
        trends[1] = trend[1] / totalTweets[1];
        trends[2] = trend[2] / totalTweets[2];
        return trends;
    }

    private void updateTrend(int keyindex, String key) {
        if (key.contains(Integer.toString(-1))) {
            trend[keyindex] -= map.get(key);
        } else if (key.contains(Integer.toString(1))) {
            trend[keyindex] += map.get(key);
        }
        totalTweets[keyindex] += map.get(key);
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

                // plot trend
                float[] trend = getCounts(map);
                dc.update(trend[0], trend[1], trend[2], time += 10);
                dc.setSize(800, 400);
                dc.setLocationRelativeTo(null);
                dc.setVisible(true);

                //cleaning of maps
                map.clear();
                treeMap.clear();
                try {
                    sleep(10000);
                } catch (InterruptedException e) {
                    System.out.println("could not sleep the thread");
                }
            }
        } finally {
            //cleaning of maps
            map.clear();
            treeMap.clear();
        }
    }
}
