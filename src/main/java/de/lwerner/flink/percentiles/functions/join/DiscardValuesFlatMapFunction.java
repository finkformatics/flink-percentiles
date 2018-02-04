package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * Function, which discards all the values from the input data set, which aren't needed anymore
 *
 * @author Lukas Werner
 */
public class DiscardValuesFlatMapFunction extends RichFlatMapFunction<Tuple3<Float, Long, Long>, Tuple3<Float, Long, Long>> {

    /**
     * Decision, if we keep the less or the greater elements
     */
    private boolean keepLess;

    /**
     * k
     */
    private long k;

    /**
     * n
     */
    private long n;

    /**
     * The weighted median
     */
    private float weightedMedian;

    @Override
    public void open(Configuration parameters) {
        Collection<Tuple3<Boolean, Long, Long>> decisionBase = getRuntimeContext().getBroadcastVariable("decisionBase");

        for (Tuple3<Boolean, Long, Long> t: decisionBase) {
            this.keepLess = t.f0;
            this.k = t.f1;
            this.n = t.f2;
        }

        Collection<Tuple1<Float>> weightedMedianCollection = getRuntimeContext().getBroadcastVariable("weightedMedian");

        for (Tuple1<Float> t: weightedMedianCollection) {
            this.weightedMedian = t.f0;
        }
    }

    @Override
    public void flatMap(Tuple3<Float, Long, Long> t, Collector<Tuple3<Float, Long, Long>> out) {
        if ((keepLess && t.f0 < weightedMedian) || (!keepLess && t.f0 > weightedMedian)) {
            out.collect(new Tuple3<>(t.f0, k, n));
        }
    }
}