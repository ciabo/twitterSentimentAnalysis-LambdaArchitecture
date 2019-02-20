package fastlayer.storm;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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
            keywordPresent = tweet.toLowerCase().contains(keywords[i]);
            i++;
        }
        if (keywordPresent == true) {
            NLP.init(); //probably the init must be put in the class initializer because is slow
            int sentiment = NLP.findSentiment(tweet);

            collector.emit(new Values(keywords[i], sentiment));
        }
    }
}
