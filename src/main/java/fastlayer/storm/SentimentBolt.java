package fastlayer.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.NLP;

public class SentimentBolt extends BaseBasicBolt {
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyword", "sentiment"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String line = tuple.getStringByField("line");
        String[] split = line.split(",");
        String tweet = split[2];
        String[] keywords = {"apple", "google", "microsoft"};
        boolean keywordPresent = false;
        int i = 0;
        while (keywordPresent == false && i < keywords.length) {
            keywordPresent = tweet.contains(keywords[i]);
        }
        if (keywordPresent == true) {
            NLP.init(); //probably the init must be put in the class initializer because is slow
            int sentiment = NLP.findSentiment(tweet);

            collector.emit(new Values(keywords[i], sentiment));
        }
    }
}
