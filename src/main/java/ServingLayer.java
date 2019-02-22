import com.datastax.driver.core.Session;
import fastlayer.cassandra.SentimentRepository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServingLayer {

    SentimentRepository repository;

    public ServingLayer(SentimentRepository repositor){
        this.repository=repository;
    }

    public Map<String,Integer> getResults(String[] keywords){
        final Map<String, Integer> fastMap = new HashMap<String, Integer>();
        for(int i=0; i <keywords.length; i++) {
            for (int j = -1; j <= 1; j++) {
                String sentiment=Integer.toString(j);
                String keyword_sentiment = keywords[i] + "-" + sentiment;
                int count=repository.selectCountFromKey("fasttable",keywords[i], j);
                fastMap.put(keyword_sentiment,count);
            }
        }

        for(int i=0; i <keywords.length; i++) {
            for (int j = -1; j <= 1; j++) {
                String sentiment=Integer.toString(j);
                String keyword_sentiment = keywords[i] + "-" + sentiment;
                int count=repository.selectCountFromKey("fasttable",keywords[i], j);
                int val = fastMap.get(keyword_sentiment);
                int newVal = val+count;
                fastMap.put(keyword_sentiment,newVal);
            }
        }
        return fastMap;

    }
}
