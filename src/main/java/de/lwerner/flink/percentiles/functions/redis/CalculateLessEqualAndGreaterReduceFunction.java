package de.lwerner.flink.percentiles.functions.redis;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Function, which reduces the set of (x, y, z) tuples, which represent for a current value if it is less (1, 0, 0),
 * equal (0, 1, 0) or greater (0, 0, 1) than the weighted median, to the sum of each element in the tuple
 *
 * @author Lukas Werner
 */
public class CalculateLessEqualAndGreaterReduceFunction implements ReduceFunction<Tuple3<Long, Long, Long>> {

    @Override
    public Tuple3<Long, Long, Long> reduce(Tuple3<Long, Long, Long> t1, Tuple3<Long, Long, Long> t2) throws Exception {
        return new Tuple3<>(t1.f0 + t2.f0, t1.f1 + t2.f1, t1.f2 + t2.f2);
    }

}