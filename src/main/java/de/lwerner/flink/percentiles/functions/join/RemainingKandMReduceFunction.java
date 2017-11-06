package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Reduces set of same k values to one value
 */
public class RemainingKandMReduceFunction implements ReduceFunction<Tuple2<Integer, Double>> {

    @Override
    public Tuple2<Integer, Double> reduce(Tuple2<Integer, Double> t1, Tuple2<Integer, Double> t2) throws Exception {
        return t1;
    }

}
