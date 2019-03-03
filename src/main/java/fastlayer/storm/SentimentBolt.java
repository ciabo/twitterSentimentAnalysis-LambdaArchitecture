package fastlayer.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import utils.NLP;
import utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class SentimentBolt extends BaseBasicBolt {
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyword", "sentiment"));
    }

    public void prepare(Map conf, TopologyContext context) {
        NLP.init();
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String line = tuple.getStringByField("line");
        String[] split = line.split(",");
        String tweet = Utils.createTweet(Arrays.copyOfRange(split, 2, split.length));
        final ArrayList<String> keywords = Utils.getKeywords();
        boolean keywordPresent = false;
        int i = 0;
        while (keywordPresent == false && i < keywords.size()) {
            keywordPresent = tweet.toLowerCase().contains(keywords.get(i));
            i++;
        }
        if (keywordPresent == true) {
            int sentiment = NLP.findSentiment(tweet);
            i--;
            collector.emit(new Values(keywords.get(i), sentiment));
        }
    }
}
