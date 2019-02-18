package mdataset;

import cascalog.CascalogFunction;
import com.twitter.maple.tap.StdoutTap;
import jcascalog.Api;
import jcascalog.Playground;
import jcascalog.Subquery;
import jcascalog.op.LT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class MDatasetQuery {
    static List ages;

    public MDatasetQuery() {
        ages = Arrays.asList(
                Arrays.asList("ettore", 22),
                Arrays.asList("marco", 55),
                Arrays.asList("luca", 18));
    }

    public static void twentyFiveYearOlds() {
        Api.execute(
                new StdoutTap(),
                new Subquery("?person", "?age")
                        .predicate(ages, "?person", "?age")
                        .predicate(new LT(), "?age", 30));
    }


    public void tweetKeywordCount(List<String> tweet, String keyword){

    }
}

