package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * Function, which discards all the values from the input data set, which aren't needed anymore
 *
 * @author Lukas Werner
 */
public class DiscardValuesFlatMapFunction extends RichFlatMapFunction<Tuple3<Double, Integer, Integer>, Tuple3<Double, Integer, Integer>> {

    /**
     * Indicates, if we already found a result
     */
    private boolean foundResult;
    /**
     * Decision, if we keep the less or the greater elements
     */
    private boolean keepLess;
    /**
     * k
     */
    private int k;
    /**
     * n
     */
    private int n;

    /**
     * The weighted median
     */
    private double weightedMedian;

    @Override
    public void open(Configuration parameters) throws Exception {
        Collection<Tuple5<Boolean, Boolean, Double, Integer, Integer>> decisionBase = getRuntimeContext().getBroadcastVariable("decisionBase");

        for (Tuple5<Boolean, Boolean, Double, Integer, Integer> t: decisionBase) {
            this.foundResult = t.f0;
            this.keepLess = t.f1;
            this.k = t.f3;
            this.n = t.f4;
        }

        Collection<Tuple1<Double>> weightedMedianCollection = getRuntimeContext().getBroadcastVariable("weightedMedian");

        for (Tuple1<Double> t: weightedMedianCollection) {
            this.weightedMedian = t.f0;
        }
    }

    @Override
    public void flatMap(Tuple3<Double, Integer, Integer> t, Collector<Tuple3<Double, Integer, Integer>> out) throws Exception {
        if ((foundResult && weightedMedian == t.f0)
                || (!foundResult && keepLess && t.f0 < weightedMedian)
                || (!foundResult && !keepLess && t.f0 > weightedMedian)) {
            out.collect(new Tuple3<>(t.f0, k, n));
        }
    }
}