package masterdataset;

import com.twitter.maple.tap.StdoutTap;
import fastlayer.cassandra.SentimentRepository;
import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.op.Count;

import java.util.List;

public class MDatasetQuery {
    static SentimentRepository smr;

    public void setSentimentRepo(SentimentRepository smr) {
        MDatasetQuery.smr = smr;
    }


    public void tweetProcessing(List tweet, String tablename) {
        smr.createTable(tablename);
        Api.execute(
                new StdoutTap(),
                new Subquery("?keyword", "?sentiment", "?count")
                        .predicate(tweet, "?tweet")
                        .predicate(new SentimentAnalysis(), "?tweet").out("?keyword", "?sentiment")
                        .predicate(new Count(), "?count")
                        .predicate(new QueryResult(), "?keyword", "?sentiment", "?count").out("?keyword", "?sentiment", "?count")
        );
    }
}

