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
    private Session session;
    private KeyspaceRepository schemaRepository;
    private SentimentRepository db;
    private String tablename;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void prepare(Map conf, TopologyContext context) {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", null);
        this.session = client.getSession();

        String keyspaceName = "tweetSentimentAnalysis";
        schemaRepository = new KeyspaceRepository(session);
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);
        schemaRepository.useKeyspace(keyspaceName);
        //create the table
        db = new SentimentRepository(session);
        tablename = "fasttable";
        db.createTable(tablename);
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String keyword = tuple.getStringByField("keyword");
        int sentiment = tuple.getIntegerByField("sentiment");
        int count = db.selectCountFromKey(tablename, keyword, sentiment);
        count++;
        db.updateCount(tablename, keyword, sentiment, count);
    }
}
