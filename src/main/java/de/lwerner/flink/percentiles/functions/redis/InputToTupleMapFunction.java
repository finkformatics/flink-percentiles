package de.lwerner.flink.percentiles.functions.redis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Function, which maps each double value, which is input to the function to a Tuple
 *
 * @author Lukas Werner
 */
public class InputToTupleMapFunction implements MapFunction<Float, Tuple1<Float>> {

    @Override
    public Tuple1<Float> map(Float f) {
        return new Tuple1<>(f);
    }

}