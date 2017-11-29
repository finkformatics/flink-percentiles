package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Function, which gets the number of less, equal and greater elements than the weighted median, and decides, what
 * to do. The decisions are:
 *   - Stop, because we found a result
 *   - Discard greater and equal values and set n to the number of less values
 *   - Discard less and equal values and set n to the number of greater values and subtract k by |less| + |equal|
 *
 * @author Lukas Werner
 */
public class DecideWhatToDoMapFunction extends RichMapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Boolean, Boolean, Double, Integer, Integer>> {

    /**
     * The weighted median
     */
    private double weightedMedian;

    @Override
    public void open(Configuration parameters) throws Exception {
        Collection<Tuple1<Double>> weightedMedian = getRuntimeContext().getBroadcastVariable("weightedMedian");
        for (Tuple1<Double> t: weightedMedian) {
            this.weightedMedian = t.f0;
        }
    }

    @Override
    public Tuple5<Boolean, Boolean, Double, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> t) throws Exception {
        boolean foundResult = false;
        boolean keepLess = false;
        double result = 0;

        int n = t.f4;
        int k = t.f3;

        if (t.f0 < t.f3 && t.f3 <= t.f0 + t.f1) {
            foundResult = true;
            result = weightedMedian;
            // We processResult the weighted median as value to our iterative data set, so we have to fetch the first element
            k = 0;
        } else if (t.f3 <= t.f0) {
            keepLess = true;
            n = t.f0;
        } else if (k > t.f0 + t.f1) {
            n = t.f2;
            k -= t.f0 + t.f1;
        }

        return new Tuple5<>(foundResult, keepLess, result, k, n);
    }

}