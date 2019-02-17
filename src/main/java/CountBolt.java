import com.datastax.driver.core.Cluster;
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

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void prepare(Map conf, TopologyContext context) {
        

    }
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String keyword = tuple.getStringByField("keyword");
        int sentiment = tuple.getIntegerByField("sentiment");


    }
}
