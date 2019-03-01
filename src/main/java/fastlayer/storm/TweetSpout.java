package fastlayer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.backtype.support.Utils;
import masterdataset.DataStore;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TweetSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> records;
    private int dbcounter;
    private FileSystem fs;
    private boolean mainFolder;

    //open is called during initialization by storm and the SpoutOutputCollector is where the output will be sent
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.fs = DataStore.configureHDFS();
        this.mainFolder=false;
        this.collector = collector;
        this.dbcounter = 0;
        String filename = "dbFast15.txt";
        this.records = new ArrayList<String>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                records.add(line);
            }
            reader.close();
        } catch (Exception e) {
            System.err.format("Exception occurred trying to read '%s'.", filename);
            e.printStackTrace();
        }
    }

    //colled again during initialization to know how the tuples generated are structured
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public void nextTuple() {
        Utils.sleep(4000);
        String line = records.get(dbcounter);
        if (dbcounter < records.size() - 1) {
            dbcounter++;
            try {
                if(this.mainFolder) {
                    System.out.println("Writing on 1.........");
                    DataStore.createAppendHDFS(fs, "tweet/newData/newTweet1.txt", line);
                }else{
                    System.out.println("Writing on 2.........");
                    DataStore.createAppendHDFS(fs, "tweet/newData/newTweet2.txt", line);
                }
            } catch (IOException e) {
                System.out.println("Error while appending newTweet");
            }
            collector.emit(new Values(line));
        }
    }

    public void changeFolder(){
        if(mainFolder) {
            this.mainFolder = false;
        }else{
            this.mainFolder=true;
        }
        int a =1;
    }

    public boolean getMainFolder(){
        return this.mainFolder;
    }


}
