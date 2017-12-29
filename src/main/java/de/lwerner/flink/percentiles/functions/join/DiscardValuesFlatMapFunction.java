package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Function, which discards all the values from the input data set, which aren't needed anymore
 *
 * @author Lukas Werner
 */
public class DiscardValuesFlatMapFunction extends RichFilterFunction<Tuple3<Float, Long, Long>> {

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
    public void open(Configuration parameters) {
        Collection<Tuple5<Boolean, Boolean, Float, Long, Long>> decisionBase = getRuntimeContext().getBroadcastVariable("decisionBase");

        for (Tuple5<Boolean, Boolean, Float, Long, Long> t: decisionBase) {
            this.foundResult = t.f0;
            this.keepLess = t.f1;
        }

        Collection<Tuple1<Float>> weightedMedianCollection = getRuntimeContext().getBroadcastVariable("weightedMedian");

        for (Tuple1<Float> t: weightedMedianCollection) {
            this.weightedMedian = t.f0;
        }
    }

    @Override
    public boolean filter(Tuple3<Float, Long, Long> t) {
        return (foundResult && weightedMedian == t.f0)
                || (!foundResult && keepLess && t.f0 < weightedMedian)
                || (!foundResult && !keepLess && t.f0 > weightedMedian);
    }
}