package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Function, which calculates the weights for each partition
 *
 * @author Lukas Werner
 */
public class CalculateWeightsMapFunction implements MapFunction<Tuple3<Float, Long, Long>, Tuple2<Float, Float>> {

    @Override
    public Tuple2<Float, Float> map(Tuple3<Float, Long, Long> medianCountAndN) {
        return new Tuple2<>(medianCountAndN.f0, medianCountAndN.f1 / (float)medianCountAndN.f2);
    }

}