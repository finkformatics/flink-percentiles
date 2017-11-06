package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Function which maps the remaining values from the Tuple to the Double value itself
 *
 * @author Lukas Werner
 */
public class RemainingValuesMapFunction implements MapFunction<Tuple3<Double, Integer, Integer>, Double> {

    @Override
    public Double map(Tuple3<Double, Integer, Integer> t) throws Exception {
        return t.f0;
    }

}