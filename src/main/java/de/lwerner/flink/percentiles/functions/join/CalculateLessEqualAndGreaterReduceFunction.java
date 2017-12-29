package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Function, which reduces the set of (x, y, z) tuples, which represent for a current value if it is less (1, 0, 0),
 * equal (0, 1, 0) or greater (0, 0, 1) than the weighted median, to the sum of each element in the tuple
 *
 * @author Lukas Werner
 */
public class CalculateLessEqualAndGreaterReduceFunction implements ReduceFunction<Tuple5<Long, Long, Long, Long, Long>> {

    @Override
    public Tuple5<Long, Long, Long, Long, Long> reduce(Tuple5<Long, Long, Long, Long, Long> t1, Tuple5<Long, Long, Long, Long, Long> t2) {
        return new Tuple5<>(t1.f0 + t2.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3, t1.f4);
    }

}