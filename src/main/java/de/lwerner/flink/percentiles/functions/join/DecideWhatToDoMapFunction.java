package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Function, which gets the number of less, equal and greater elements than the weighted median, and decides, what
 * to do. The decisions are:
 *   - Stop, because we found a result
 *   - Discard greater and equal values and set n to the number of less values
 *   - Discard less and equal values and set n to the number of greater values and subtract k by |less| + |equal|
 *
 * @author Lukas Werner
 */
public class DecideWhatToDoMapFunction implements MapFunction<Tuple5<Long, Long, Long, Long, Long>, Tuple3<Boolean, Long, Long>> {

    @Override
    public Tuple3<Boolean, Long, Long> map(Tuple5<Long, Long, Long, Long, Long> t) {
        boolean keepLess = false;

        long n = t.f4;
        long k = t.f3;

        if (t.f3 <= t.f0) {
            keepLess = true;
            n = t.f0;
        } else if (k > t.f0 + t.f1) {
            n = t.f2;
            k -= t.f0 + t.f1;
        }

        return new Tuple3<>(keepLess, k, n);
    }

}