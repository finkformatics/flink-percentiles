package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Function, which reduces the set of (x, y, z) tuples, which represent for a current value if it is less (1, 0, 0),
 * equal (0, 1, 0) or greater (0, 0, 1) than the weighted median, to the sum of each element in the tuple
 *
 * @author Lukas Werner
 */
public class CalculateLessEqualAndGreaterReduceFunction implements ReduceFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>> {

    @Override
    public Tuple5<Integer, Integer, Integer, Integer, Integer> reduce(Tuple5<Integer, Integer, Integer, Integer, Integer> t1, Tuple5<Integer, Integer, Integer, Integer, Integer> t2) throws Exception {
        return new Tuple5<>(t1.f0 + t2.f0, t1.f1 + t2.f1, t1.f2 + t2.f2, t1.f3, t1.f4);
    }

}