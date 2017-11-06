package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Function, which calculates the weights for each partition
 *
 * @author Lukas Werner
 */
public class CalculateWeightsMapFunction implements MapFunction<Tuple3<Double, Integer, Integer>, Tuple2<Double, Double>> {

    @Override
    public Tuple2<Double, Double> map(Tuple3<Double, Integer, Integer> medianCountAndN) throws Exception {
        return new Tuple2<>(medianCountAndN.f0, medianCountAndN.f1 / (double)medianCountAndN.f2);
    }

}