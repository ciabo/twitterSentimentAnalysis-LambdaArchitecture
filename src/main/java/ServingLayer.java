import com.datastax.driver.core.Session;
import fastlayer.cassandra.SentimentRepository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServingLayer {

    static public Map<String, Integer> getResults(String[] keywords) {
        SentimentRepository repository = SentimentRepository.getInstance();
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < keywords.length; i++) {
            for (int j = -1; j <= 1; j++) {
                String sentiment = Integer.toString(j);
                String keyword_sentiment = keywords[i] + " | " + sentiment;
                int count = repository.selectCountFromKey("fasttable", keywords[i], j);
                map.put(keyword_sentiment, count);
            }
        }

        for (int i = 0; i < keywords.length; i++) {
            for (int j = -1; j <= 1; j++) {
                String sentiment = Integer.toString(j);
                String keyword_sentiment = keywords[i] + " | " + sentiment;
                int count = repository.selectCountFromKey("batchtable", keywords[i], j);
                int val = map.get(keyword_sentiment);
                int newVal = val + count;
                map.put(keyword_sentiment, newVal);
            }
        }
        return map;
    }
}
