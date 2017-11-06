package de.lwerner.flink.percentiles.functions.redis;

import de.lwerner.flink.percentiles.model.DecisionModel;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Function, which discards all the values from the input data set, which aren't needed anymore
 *
 * @author Lukas Werner
 */
public class DiscardValuesFilterFunction extends RichFilterFunction<Tuple1<Float>> {

    /**
     * Indicates, if we already found a result
     */
    private boolean foundResult;
    /**
     * Decision, if we keep the less or the greater elements
     */
    private boolean keepLess;

    /**
     * The weighted median
     */
    private float weightedMedian;

    @Override
    public void open(Configuration parameters) throws Exception {
        Collection<DecisionModel> decisionBase = getRuntimeContext().getBroadcastVariable("decisionBase");

        for (DecisionModel decisionModel: decisionBase) {
            foundResult = decisionModel.isFoundResult();
            keepLess = decisionModel.isKeepLess();
        }

        Collection<Tuple1<Float>> weightedMedianCollection = getRuntimeContext().getBroadcastVariable("weightedMedian");

        for (Tuple1<Float> t: weightedMedianCollection) {
            weightedMedian = t.f0;
        }
    }

    @Override
    public boolean filter(Tuple1<Float> t) throws Exception {
        return !foundResult && ((keepLess && t.f0 < weightedMedian) || (!keepLess && t.f0 > weightedMedian));
    }
}