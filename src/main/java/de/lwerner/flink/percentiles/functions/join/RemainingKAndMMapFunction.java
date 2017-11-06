package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Maps a tuple like (value, k, n) to k
 */
public class RemainingKAndMMapFunction implements MapFunction<Tuple4<Double, Integer, Integer, Double>, Tuple2<Integer, Double>> {

    @Override
    public Tuple2<Integer, Double> map(Tuple4<Double, Integer, Integer, Double> t) throws Exception {
        return new Tuple2<>(t.f1, t.f3);
    }

}