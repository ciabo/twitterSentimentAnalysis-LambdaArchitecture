package masterdataset;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import utils.NLP;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SentimentAnalysis extends CascalogFunction {
    private static final List<String> keywords = Arrays.asList("apple", "google", "microsoft");

    @Override
    public void operate(FlowProcess process, FunctionCall call) {
        //System.out.println("I'm searching for: " + keywords);
        String tweet = call.getArguments().getString(0);
        NLP.init();
        int sentiment = NLP.findSentiment(tweet);
        for (String t : keywords)
            if (tweet.contains(t))
                call.getOutputCollector().add(new Tuple(t, sentiment));
    }
}