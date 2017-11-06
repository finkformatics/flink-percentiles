package de.lwerner.flink.percentiles.functions.redis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Function which maps the remaining values from the Tuple to the Double value itself
 *
 * @author Lukas Werner
 */
public class RemainingValuesMapFunction implements MapFunction<Tuple1<Float>, Float> {

    @Override
    public Float map(Tuple1<Float> t) throws Exception {
        return t.f0;
    }

}