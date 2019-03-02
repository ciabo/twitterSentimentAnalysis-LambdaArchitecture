import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.backtype.hadoop.pail.Pail;
import com.datastax.driver.core.Session;
import fastlayer.cassandra.CassandraConnector;
import fastlayer.cassandra.KeyspaceRepository;
import fastlayer.cassandra.SentimentRepository;
import fastlayer.storm.CountBolt;
import fastlayer.storm.SentimentBolt;
import fastlayer.storm.TweetSpout;
import masterdataset.DataStore;
import masterdataset.MDatasetQuery;
import masterdataset.TweetStructure;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Init {
    private LocalCluster cluster;
    private Config conf;
    private TopologyBuilder builder;
    private FileSystem fs;
    private MDatasetQuery mq;
    private Pail tweetPail;
    private Pail newTweetPail;
    private String newpath = "hdfs://localhost:9000/user/ettore/pail/tweet/newData";
    private String batchPath = "hdfs://localhost:9000/user/ettore/pail/tweet/batchTweet";

    public Init() throws IOException {

        //cassandra cluster init
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", null);
        Session session = client.getSession();

        // keyspace of batch and fast layer
        String keyspaceName = "tweetSentimentAnalysis";
        KeyspaceRepository schemaRepository = new KeyspaceRepository(session);
        schemaRepository.useKeyspace(keyspaceName);

        // create fastlayer table
        SentimentRepository db = SentimentRepository.getInstance(session);

        db.deleteTable("fasttable");
        db.deleteTable("batchtable");
        db.createTable("fasttable");
        db.createTable("batchtable");

        // configure and set file to store(batch) new fastlayer's tweets
        fs = DataStore.configureHDFS();
        fs.delete(new Path("/user/ettore/pail/tweet"), true);

        try {
            tweetPail = Pail.create(batchPath, new TweetStructure());
            newTweetPail = Pail.create(newpath, new TweetStructure());
        } catch (IOException e) {
            System.out.println("Unable to create bacth tweet Pail");
        }

        // storm init
        builder = new TopologyBuilder();
        builder.setSpout("tweet_spout", new TweetSpout(), 1);
        builder.setBolt("sentiment_bolt", new SentimentBolt(), 4).shuffleGrouping("tweet_spout");
        builder.setBolt("count_bolt", new CountBolt(), 4).fieldsGrouping("sentiment_bolt", new Fields("keyword", "sentiment"));
        cluster = new LocalCluster();
        conf = new Config();
        conf.setDebug(true);

        mq = new MDatasetQuery();
    }

    public LocalCluster getCluster() {
        return cluster;
    }

    public Config getConf() {
        return conf;
    }

    public TopologyBuilder getBuilder() {
        return builder;
    }

    public FileSystem getFs() {
        return fs;
    }

    public MDatasetQuery getMq() {
        return mq;
    }

    public String getBatchPath() {
        return batchPath;
    }
}
