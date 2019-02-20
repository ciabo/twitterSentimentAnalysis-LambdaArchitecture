package masterdataset;

import com.twitter.maple.tap.StdoutTap;
import fastlayer.cassandra.SentimentRepository;
import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.op.Count;

import java.util.List;

public class MDatasetQuery {
    public static void tweetProcessing(List tweet, List smr) {
        Api.execute(
                new StdoutTap(),
                new Subquery("?keyword", "?sentiment", "?count")
                        .predicate(tweet, "?tweet")
                        .predicate(new SentimentAnalysis(), "?tweet").out("?keyword", "?sentiment")
                        .predicate(new Count(), "?count")
                        .predicate(smr, "?smr")
                        .predicate(new QueryResult(), "?keyword", "?sentiment", "?count", "?smr").out("?keyword", "?sentiment", "?count")
        );
    }
}

