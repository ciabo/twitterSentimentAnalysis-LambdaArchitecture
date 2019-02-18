import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


import java.util.Map;

public class CountBolt extends BaseBasicBolt {
    private Session session;
    private KeyspaceRepository schemaRepository;
    private SentimentRepository db;
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void prepare(Map conf, TopologyContext context) {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", null);
        this.session=client.getSession();

        String keyspaceName = "tweetSentimentAnalysis";
        schemaRepository = new KeyspaceRepository(session);
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);
        schemaRepository.useKeyspace(keyspaceName);
        //create the table
        db = new SentimentRepository(session);
        db.createTable();


    }
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String keyword = tuple.getStringByField("keyword");
        int sentiment = tuple.getIntegerByField("sentiment");
        int count = db.selectCountFromKey(keyword,sentiment);
        count++;
        db.updateCount(keyword,sentiment,count);
    }
}
