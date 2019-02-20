package masterdataset;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import fastlayer.cassandra.SentimentRepository;

public class QueryResult extends CascalogFunction {
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        String keyword = functionCall.getArguments().getString(0);
        int sentiment = functionCall.getArguments().getInteger(1);
        int count = functionCall.getArguments().getInteger(2);
        SentimentRepository smr = (SentimentRepository) functionCall.getArguments().get(3);
        smr.updateCount("batchtable", keyword, sentiment, count);
        functionCall.getOutputCollector().add(new Tuple(keyword, sentiment, count));
    }
}
