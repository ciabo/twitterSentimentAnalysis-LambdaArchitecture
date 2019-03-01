import fastlayer.cassandra.SentimentRepository;

import java.util.HashMap;
import java.util.Map;

public class ServingLayer {

    static public Map<String, Long> getResults(String[] keywords) {
        SentimentRepository repository = SentimentRepository.getInstance();
        Map<String, Long> map = new HashMap<String, Long>();
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
        return map;
    }
}
