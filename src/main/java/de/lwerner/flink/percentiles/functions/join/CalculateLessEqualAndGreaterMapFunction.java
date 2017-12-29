package de.lwerner.flink.percentiles.functions.join;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Function which maps each double value to a tuple (x, y, z) which says, if the element is less (1, 0, 0), equal
 * (0, 1, 0) or greater (0, 0, 1) than the weighted median.
 *
 * @author Lukas Werner
 */
public class CalculateLessEqualAndGreaterMapFunction extends RichMapFunction<Tuple3<Float, Long, Long>, Tuple5<Long, Long, Long, Long, Long>> {

    /**
     * The weighted median
     */
    private float weightedMedian;

    @Override
    public void open(Configuration parameters) {
        Collection<Tuple1<Float>> weightedMedian = getRuntimeContext().getBroadcastVariable("weightedMedian");

        for (Tuple1<Float> t: weightedMedian) {
            this.weightedMedian = t.f0;
        }
    }

    @Override
    public Tuple5<Long, Long, Long, Long, Long> map(Tuple3<Float, Long, Long> t) {
        return new Tuple5<>(t.f0 < weightedMedian ? 1L : 0L, t.f0 == weightedMedian ? 1L : 0L, t.f0 > weightedMedian ? 1L : 0L, t.f1, t.f2);
    }

}