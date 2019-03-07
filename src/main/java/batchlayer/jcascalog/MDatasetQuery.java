package batchlayer.jcascalog;

import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.op.Count;

import java.util.List;

public class MDatasetQuery {
    public void tweetProcessing(List tweet) {
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

