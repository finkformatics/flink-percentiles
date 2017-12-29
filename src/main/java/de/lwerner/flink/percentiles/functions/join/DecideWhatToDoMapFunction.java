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
public class DecideWhatToDoMapFunction extends RichMapFunction<Tuple5<Long, Long, Long, Long, Long>, Tuple5<Boolean, Boolean, Float, Long, Long>> {

    /**
     * The weighted median
     */
    private float weightedMedian;

    @Override
    public void open(Configuration parameters) {
        Collection<Tuple1<Float>> weightedMedian = getRuntimeContext().getBroadcastVariable("weightedMedian");
        for (Tuple1<Float> t: weightedMedian) {
            this.weightedMedian = t.f0;
        }
    }

    @Override
    public Tuple5<Boolean, Boolean, Float, Long, Long> map(Tuple5<Long, Long, Long, Long, Long> t) {
        boolean foundResult = false;
        boolean keepLess = false;
        float result = 0;

        long n = t.f4;
        long k = t.f3;

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