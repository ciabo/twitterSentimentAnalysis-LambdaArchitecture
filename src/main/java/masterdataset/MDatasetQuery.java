package masterdataset;

import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Playground;
import jcascalog.Subquery;
import jcascalog.example.Split;
import jcascalog.op.Count;
import jcascalog.op.GT;
import org.apache.hadoop.util.DataChecksum;

import java.util.List;

public class MDatasetQuery {

    public static void wordCount() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?word", "?count")
                        .predicate(Playground.SENTENCE, "?sentence")
                        .predicate(new Split(), "?sentence").out("?word")
                        .predicate(new Count(), "?count"));
    }

    public static void tweetProcessing(List tweet) {
        Api.execute(
                new StdoutTap(),
                new Subquery("?keyword", "?sentiment", "?count")
                        .predicate(tweet, "?tweet")
                        .predicate(new SentimentAnalysis(), "?tweet").out("?keyword", "?sentiment")
                        .predicate(new Count(), "?count")
        );
    }
}

