package masterdataset;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import utils.NLP;
import utils.Utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SentimentAnalysis extends CascalogFunction {
    private final List<String> keywords = Utils.getKeywords();

    @Override
    public void operate(FlowProcess process, FunctionCall call) {
        String tweet = call.getArguments().getString(0);
        NLP.init();
        int sentiment = NLP.findSentiment(tweet);
        for (String t : keywords)
            if (tweet.toLowerCase().contains(t))
                call.getOutputCollector().add(new Tuple(t, sentiment));
    }
}