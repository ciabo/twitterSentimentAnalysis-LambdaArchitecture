package fastlayer.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import fastlayer.cassandra.CassandraConnector;
import fastlayer.cassandra.KeyspaceRepository;
import fastlayer.cassandra.SentimentRepository;

import java.util.Map;

public class CountBolt extends BaseBasicBolt {
    private SentimentRepository db;
    private String tablename;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void prepare(Map conf, TopologyContext context) {
        tablename = "fasttable";
        db = SentimentRepository.getInstance();
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String keyword = tuple.getStringByField("keyword");
        int sentiment = tuple.getIntegerByField("sentiment");
        db.addToCount(tablename, keyword, sentiment, 1);
    }
}
