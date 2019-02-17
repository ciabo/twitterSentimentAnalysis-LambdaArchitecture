import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.db.Keyspace;
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
    //schemarepository to be created
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void prepare(Map conf, TopologyContext context) {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9142);
        //create the column family
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append("sentimentCount").append("(")
                .append("keyword text, ")
                .append("sentiment int,")
                .append("count int")
                .append("PRIMARY KEY (keyword, sentiment) );");
        this.session = client.getSession();
        String query = sb.toString();
        session.execute(query);

    }
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String keyword = tuple.getStringByField("keyword");
        int sentiment = tuple.getIntegerByField("sentiment");


    }
}
