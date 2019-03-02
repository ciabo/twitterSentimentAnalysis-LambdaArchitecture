import fastlayer.cassandra.SentimentRepository;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


import static java.lang.Thread.sleep;

public class ServingLayer implements Runnable {
    private String[] keywords;

    public ServingLayer(String[] keywords) {
        this.keywords = keywords;
    }

    @Override
    public void run() {
        SentimentRepository repository = SentimentRepository.getInstance();
        Map<String, Long> map = new HashMap<String, Long>();
        Map<String, Long> treeMap;
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
            map.clear();
            treeMap.clear();


        }
    }
}
